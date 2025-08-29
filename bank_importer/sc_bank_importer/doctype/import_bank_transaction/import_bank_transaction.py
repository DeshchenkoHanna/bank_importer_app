# Copyright (c) 2025, Swiss Cluster and contributors
# For license information, please see license.txt

# import frappe
import frappe
import xml.etree.ElementTree as ET
from datetime import datetime
from difflib import SequenceMatcher
import os
import glob
import requests
import re
from urllib.parse import urlparse
import zipfile
import io

from frappe.model.document import Document


class ImportBankTransaction(Document):
	pass

def extract_party_from_structured_data(ntry_element, namespaces):
    """Extract party information from structured CAMT.053 elements according to UBS format"""
    party_info = {
        "debtor_name": None,
        "debtor_account": None,
        "creditor_name": None, 
        "creditor_account": None,
        "debtor_agent_name": None,
        "creditor_agent_name": None,
        "ultimate_debtor": None,
        "ultimate_creditor": None
    }
    
    # Look for transaction details with related parties
    tx_dtls = ntry_element.find('./camt:NtryDtls/camt:TxDtls', namespaces)
    if tx_dtls is None:
        return party_info
    
    rltd_parties = tx_dtls.find('./camt:RltdPties', namespaces)
    if rltd_parties is None:
        return party_info
    
    # Extract Debtor information
    debtor = rltd_parties.find('./camt:Dbtr', namespaces)
    if debtor is not None:
        # Try to get debtor name directly from Nm element (UBS format)
        debtor_name = debtor.findtext('./camt:Nm', namespaces=namespaces)
        if debtor_name:
            party_info["debtor_name"] = debtor_name
        else:
            # Try to get debtor name from Party element (alternative format)
            debtor_party = debtor.find('./camt:Pty', namespaces)
            if debtor_party is not None:
                party_info["debtor_name"] = debtor_party.findtext('./camt:Nm', namespaces=namespaces)
            
            # Fallback: Try to get debtor name from postal address AdrLine
            if not party_info["debtor_name"]:
                postal_addr = debtor.find('./camt:PstlAdr', namespaces)
                if postal_addr is not None:
                    adr_lines = postal_addr.findall('./camt:AdrLine', namespaces)
                    for adr_line in adr_lines:
                        if adr_line is not None and adr_line.text:
                            # Use first non-empty address line as potential party name
                            # Skip address lines that look like addresses (contain numbers, postal codes)
                            line_text = adr_line.text.strip()
                            if line_text and not any(char.isdigit() for char in line_text[:10]):  # Check first 10 chars for digits
                                party_info["debtor_name"] = line_text
                                break
    
    # Extract Debtor Account
    debtor_acct = rltd_parties.find('./camt:DbtrAcct', namespaces)
    if debtor_acct is not None:
        iban = debtor_acct.findtext('./camt:Id/camt:IBAN', namespaces=namespaces)
        if iban:
            party_info["debtor_account"] = iban
        else:
            other_id = debtor_acct.findtext('./camt:Id/camt:Othr/camt:Id', namespaces=namespaces)
            if other_id:
                party_info["debtor_account"] = other_id
    
    # Extract Creditor information
    creditor = rltd_parties.find('./camt:Cdtr', namespaces)
    if creditor is not None:
        # Try to get creditor name directly from Nm element (UBS format)
        creditor_name = creditor.findtext('./camt:Nm', namespaces=namespaces)
        if creditor_name:
            party_info["creditor_name"] = creditor_name
        else:
            # Try to get creditor name from Party element (alternative format)
            creditor_party = creditor.find('./camt:Pty', namespaces)
            if creditor_party is not None:
                party_info["creditor_name"] = creditor_party.findtext('./camt:Nm', namespaces=namespaces)
            
            # Fallback: Try to get creditor name from postal address AdrLine
            if not party_info["creditor_name"]:
                postal_addr = creditor.find('./camt:PstlAdr', namespaces)
                if postal_addr is not None:
                    adr_lines = postal_addr.findall('./camt:AdrLine', namespaces)
                    for adr_line in adr_lines:
                        if adr_line is not None and adr_line.text:
                            # Use first non-empty address line as potential party name
                            # Skip address lines that look like addresses (contain numbers, postal codes)
                            line_text = adr_line.text.strip()
                            if line_text and not any(char.isdigit() for char in line_text[:10]):  # Check first 10 chars for digits
                                party_info["creditor_name"] = line_text
                                break
    
    # Extract Creditor Account  
    creditor_acct = rltd_parties.find('./camt:CdtrAcct', namespaces)
    if creditor_acct is not None:
        iban = creditor_acct.findtext('./camt:Id/camt:IBAN', namespaces=namespaces)
        if iban:
            party_info["creditor_account"] = iban
        else:
            other_id = creditor_acct.findtext('./camt:Id/camt:Othr/camt:Id', namespaces=namespaces)
            if other_id:
                party_info["creditor_account"] = other_id
    
    # Extract Ultimate Debtor
    ultimate_debtor = rltd_parties.find('./camt:UltmtDbtr', namespaces)
    if ultimate_debtor is not None:
        party_info["ultimate_debtor"] = ultimate_debtor.findtext('./camt:Nm', namespaces=namespaces)
    
    # Extract Ultimate Creditor
    ultimate_creditor = rltd_parties.find('./camt:UltmtCdtr', namespaces)
    if ultimate_creditor is not None:
        party_info["ultimate_creditor"] = ultimate_creditor.findtext('./camt:Nm', namespaces=namespaces)
    
    # Extract Related Agents information
    rltd_agents = tx_dtls.find('./camt:RltdAgts', namespaces)
    if rltd_agents is not None:
        # Debtor Agent
        debtor_agent = rltd_agents.find('./camt:DbtrAgt', namespaces)
        if debtor_agent is not None:
            party_info["debtor_agent_name"] = debtor_agent.findtext('./camt:FinInstnId/camt:Nm', namespaces=namespaces)
        
        # Creditor Agent
        creditor_agent = rltd_agents.find('./camt:CdtrAgt', namespaces)
        if creditor_agent is not None:
            party_info["creditor_agent_name"] = creditor_agent.findtext('./camt:FinInstnId/camt:Nm', namespaces=namespaces)
    
    return party_info

def find_party_from_structured_data(party_info, cdt_dbt_indicator):
    """Find ERPNext party from structured party information"""
    
    # Determine which party name to use based on transaction direction
    party_names_to_search = []
    
    if cdt_dbt_indicator == 'DBIT':  # Money going out - look for creditor/supplier
        if party_info["creditor_name"]:
            party_names_to_search.append(party_info["creditor_name"])
        if party_info["ultimate_creditor"]:
            party_names_to_search.append(party_info["ultimate_creditor"])
    elif cdt_dbt_indicator == 'CRDT':  # Money coming in - look for debtor/customer  
        if party_info["debtor_name"]:
            party_names_to_search.append(party_info["debtor_name"])
        if party_info["ultimate_debtor"]:
            party_names_to_search.append(party_info["ultimate_debtor"])
    
    # Search for party matches
    for party_name in party_names_to_search:
        if not party_name:
            continue
            
        # First try exact matches with custom_name_in_bank
        result = find_exact_bank_name_match(party_name)
        if result:
            return result
            
        # Then try partial matches and fuzzy matching
        result = find_fuzzy_party_match(party_name)
        if result:
            return result
    
    return None

def find_exact_bank_name_match(bank_name):
    """Find exact match using custom_name_in_bank field"""
    if not bank_name:
        return None
    
    # Check customers first
    customer = frappe.db.get_value("Customer", 
        {"custom_name_in_bank": bank_name.strip(), "disabled": 0}, 
        "name")
    if customer:
        return {"party_type": "Customer", "party": customer}
    
    # Check suppliers
    supplier = frappe.db.get_value("Supplier", 
        {"custom_name_in_bank": bank_name.strip(), "disabled": 0}, 
        "name")
    if supplier:
        return {"party_type": "Supplier", "party": supplier}
    
    return None

def find_fuzzy_party_match(search_name):
    """Find party using fuzzy matching logic"""
    if not search_name:
        return None
    
    search_name = search_name.strip().lower()
    
    # First try partial string matching
    # Check customers
    customers = frappe.get_all("Customer", 
        fields=["name", "customer_name"], 
        filters={"disabled": 0})
    
    for customer in customers:
        if customer.customer_name and customer.customer_name.lower() in search_name:
            return {"party_type": "Customer", "party": customer.name}
    
    # Check suppliers  
    suppliers = frappe.get_all("Supplier",
        fields=["name", "supplier_name"], 
        filters={"disabled": 0})
    
    for supplier in suppliers:
        if supplier.supplier_name and supplier.supplier_name.lower() in search_name:
            return {"party_type": "Supplier", "party": supplier.name}
    
    # Fallback to sequence matching with higher threshold for structured data
    best_match = None
    highest_score = 0.85  # Higher threshold for structured data
    
    all_parties = []
    for c in customers:
        if c.customer_name:
            all_parties.append({"name": c.name, "search_name": c.customer_name, "type": "Customer"})
    for s in suppliers:
        if s.supplier_name:
            all_parties.append({"name": s.name, "search_name": s.supplier_name, "type": "Supplier"})
    
    for party_info in all_parties:
        score = SequenceMatcher(None, party_info["search_name"].lower(), search_name).ratio()
        if score > highest_score:
            highest_score = score
            best_match = {"party_type": party_info["type"], "party": party_info["name"]}
    
    return best_match

def find_party_from_details(description):
    """Legacy function - enhanced to work with custom_name_in_bank"""
    if not description:
        return None
    
    # First try custom_name_in_bank exact matches
    customers_with_bank_name = frappe.get_all("Customer", 
        fields=["name", "custom_name_in_bank"], 
        filters={"custom_name_in_bank": ["!=", ""], "disabled": 0})
    
    for customer in customers_with_bank_name:
        if customer.custom_name_in_bank and customer.custom_name_in_bank.lower() in description.lower():
            return {"party_type": "Customer", "party": customer.name}

    suppliers_with_bank_name = frappe.get_all("Supplier", 
        fields=["name", "custom_name_in_bank"], 
        filters={"custom_name_in_bank": ["!=", ""], "disabled": 0})
    
    for supplier in suppliers_with_bank_name:
        if supplier.custom_name_in_bank and supplier.custom_name_in_bank.lower() in description.lower():
            return {"party_type": "Supplier", "party": supplier.name}
        
    # Continue with original fuzzy logic for fallback
    best_match = None
    highest_score = 0.9
    parties_to_check = []
    
    customers = frappe.get_all("Customer", fields=["name", "customer_name"], filters={"disabled": 0})
    for c in customers:
        if c.customer_name:
            parties_to_check.append({"name": c.name, "search_name": c.customer_name, "type": "Customer"})
            
    suppliers = frappe.get_all("Supplier", fields=["name", "supplier_name"], filters={"disabled": 0})
    for s in suppliers:
        if s.supplier_name:
            parties_to_check.append({"name": s.name, "search_name": s.supplier_name, "type": "Supplier"})
    
    for party_info in parties_to_check:
        if party_info["search_name"].lower() in description.lower():
            return {"party_type": party_info["type"], "party": party_info["name"]}
            
        for word in description.split():
            score = SequenceMatcher(None, party_info["search_name"].lower(), word.lower()).ratio()
            if score > highest_score:
                highest_score = score
                best_match = {"party_type": party_info["type"], "party": party_info["name"]}
    
    return best_match

def find_existing_transaction(reference_number):
    """Checks if a Bank Transaction with the same reference number already exists."""
    if not reference_number:
        return None
    return frappe.db.get_value("Bank Transaction", {"reference_number": reference_number, "docstatus": 1}, "name")

def find_bank_account_by_iban(iban):
    """Find bank account by IBAN, handling space formatting differences."""
    if not iban:
        return None
    
    # Normalize IBAN by removing spaces and converting to uppercase
    normalized_iban = iban.replace(" ", "").upper()
    
    # First try direct match
    direct_match = frappe.db.get_value("Bank Account", {"iban": iban}, "name")
    if direct_match:
        return direct_match
    
    # Get all bank accounts with IBANs and check normalized versions
    all_accounts = frappe.get_all("Bank Account", 
        fields=["name", "iban"], 
        filters={"iban": ["!=", ""]})
    
    for account in all_accounts:
        if account.iban:
            # Normalize stored IBAN and compare
            stored_normalized = account.iban.replace(" ", "").upper()
            if stored_normalized == normalized_iban:
                return account.name
    
    return None

def format_qrr_reference(qrr_ref):
    """Format QRR reference number into Swiss standard format with spaces"""
    if not qrr_ref:
        return qrr_ref
    
    # Remove any existing spaces and ensure it's a string
    clean_ref = str(qrr_ref).replace(" ", "").strip()
    
    # QRR references should be 27 digits long
    if len(clean_ref) != 27 or not clean_ref.isdigit():
        return qrr_ref  # Return original if not a valid QRR format
    
    # Format: XX XXXXX XXXXX XXXXX XXXXX XXXXX (2-5-5-5-5-5 digits)
    formatted = f"{clean_ref[0:2]} {clean_ref[2:7]} {clean_ref[7:12]} {clean_ref[12:17]} {clean_ref[17:22]} {clean_ref[22:27]}"
    
    return formatted

def get_bank_account_iban(bank_account_name):
    """Get IBAN from ERPNext Bank Account record"""
    if not bank_account_name:
        return None
    
    try:
        bank_account = frappe.get_doc("Bank Account", bank_account_name)
        iban = bank_account.iban if bank_account else None
        # DEBUG: Log what we found
        frappe.log_error(f"Bank Account: '{bank_account_name}' -> IBAN: '{iban}'", "Get Bank Account IBAN Debug")
        return iban
    except Exception as e:
        frappe.log_error(f"Error getting Bank Account IBAN for '{bank_account_name}': {str(e)}", "Get Bank Account IBAN Debug")
        return None

def validate_file_bank_account(file_content, reference_iban):
    """Extract IBAN from file and compare with reference bank account IBAN"""
    if not reference_iban:
        frappe.log_error(f"No reference IBAN provided - accepting all files", "Bank Account Validation Debug")
        return True  # If no reference IBAN, accept all files
    
    try:
        # Detect CAMT version from file content
        namespaces = {'camt': 'urn:iso:std:iso:20022:tech:xsd:camt.053.001.02'}  # default
        file_content_str = str(file_content)
        if 'camt.053.001.10' in file_content_str:
            namespaces = {'camt': 'urn:iso:std:iso:20022:tech:xsd:camt.053.001.10'}
        elif 'camt.053.001.04' in file_content_str:
            namespaces = {'camt': 'urn:iso:std:iso:20022:tech:xsd:camt.053.001.04'}
        elif 'camt.053.001.08' in file_content_str:
            namespaces = {'camt': 'urn:iso:std:iso:20022:tech:xsd:camt.053.001.08'}
        
        root = ET.fromstring(file_content)
        
        # Extract IBAN from first statement
        for stmt_element in root.findall('.//camt:BkToCstmrStmt/camt:Stmt', namespaces):
            file_iban = stmt_element.findtext('./camt:Acct/camt:Id/camt:IBAN', default="", namespaces=namespaces)
            if file_iban:
                # Normalize both IBANs by removing spaces and converting to uppercase
                file_iban_normalized = file_iban.replace(" ", "").strip().upper()
                reference_iban_normalized = reference_iban.replace(" ", "").strip().upper()
                match_result = file_iban_normalized == reference_iban_normalized
                frappe.log_error(f"IBAN Comparison - File: '{file_iban_normalized}' vs Reference: '{reference_iban_normalized}' -> Match: {match_result}", "Bank Account Validation Debug")
                return match_result
        
        frappe.log_error(f"No IBAN found in file. Reference IBAN: '{reference_iban}'", "Bank Account Validation Debug")
        return False  # No IBAN found in file
    except Exception as e:
        frappe.log_error(f"Exception in validate_file_bank_account: {str(e)}. Reference IBAN: '{reference_iban}'", "Bank Account Validation Debug")
        return False  # Invalid file format

def is_cloud_storage_url(path):
    """Check if the path is a cloud storage URL"""
    if not path:
        return False
    
    # Check if it's a URL
    parsed = urlparse(path)
    if parsed.scheme in ['http', 'https']:
        # Check for common cloud storage patterns
        cloud_patterns = [
            'dropbox.com',
            'drive.google.com',
            'onedrive.live.com',
            'sharepoint.com',
            '1drv.ms',
            'box.com',
            'icloud.com'
        ]
        return any(pattern in parsed.netloc.lower() for pattern in cloud_patterns)
    
    return False

def extract_google_drive_folder_id(url):
    """Extract folder ID from Google Drive folder URL"""
    # Handle different Google Drive folder URL formats
    patterns = [
        r'/folders/([a-zA-Z0-9-_]+)',  # Standard format
        r'id=([a-zA-Z0-9-_]+)',       # Alternative format
        r'folderview\?id=([a-zA-Z0-9-_]+)'  # Old format
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    return None

def get_google_drive_folder_files(folder_url):
    """Get all XML files from a Google Drive folder"""
    files_data = []
    
    try:
        # Extract folder ID
        folder_id = extract_google_drive_folder_id(folder_url)
        if not folder_id:
            frappe.throw("Could not extract Google Drive folder ID from URL")
        
        # For public Google Drive folders, use a simple approach
        # Make the folder publicly viewable and accessible
        
        # Try to access folder content by converting to a different URL format
        # This works for publicly shared folders
        try:
            # Convert folder URL to a format we can process
            folder_view_url = f"https://drive.google.com/drive/folders/{folder_id}"
            
            # Add headers to mimic browser request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(folder_view_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Look for XML files in the response
            # This is a simplified approach - extract file IDs from the HTML
            html_content = response.text
            
            # Find file patterns in the HTML (this is fragile but works for basic cases)
            file_pattern = r'"([a-zA-Z0-9-_]{25,})"[^"]*\.xml'
            file_matches = re.findall(file_pattern, html_content)
            
            if not file_matches:
                # Try alternative pattern
                file_pattern = r'data-id="([a-zA-Z0-9-_]{25,})"[^>]*>[^<]*\.xml'
                file_matches = re.findall(file_pattern, html_content)
            
            if file_matches:
                for file_id in file_matches[:10]:  # Limit to 10 files for safety
                    try:
                        # Download each file
                        download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
                        file_response = requests.get(download_url, headers=headers, timeout=30)
                        file_response.raise_for_status()
                        
                        # Create filename
                        filename = f"gdrive_file_{file_id}.xml"
                        
                        files_data.append({
                            'filename': filename,
                            'content': file_response.content
                        })
                    except:
                        continue  # Skip failed downloads
            
            if not files_data:
                frappe.throw(
                    "No XML files found in the Google Drive folder. Please ensure:\n"
                    "1. The folder is publicly accessible\n"
                    "2. The folder contains .xml files\n"
                    "3. The sharing settings allow file downloads\n\n"
                    "Alternative: Download files locally and use local folder path."
                )
        
        except Exception as web_error:
            frappe.throw(
                f"Cannot access Google Drive folder. Please try one of these alternatives:\n"
                f"1. Download files locally and use local folder path\n"
                f"2. Share individual file download links instead of folder link\n"
                f"3. Use a different cloud storage service\n\n"
                f"Error details: {str(web_error)}"
            )
        
    except Exception as e:
        frappe.throw(f"Error processing Google Drive folder: {str(e)}")
    
    return files_data

def get_files_from_cloud_storage(storage_url):
    """Get XML file contents from cloud storage URL"""
    files_data = []
    
    try:
        # For direct file links, download the file
        if storage_url.endswith('.xml') or 'export=download' in storage_url:
            # Handle Google Drive direct download links
            if 'drive.google.com' in storage_url:
                # Convert Google Drive share links to direct download
                if '/file/d/' in storage_url:
                    file_id = storage_url.split('/file/d/')[1].split('/')[0]
                    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
                else:
                    download_url = storage_url
            else:
                download_url = storage_url
            
            response = requests.get(download_url, timeout=30)
            response.raise_for_status()
            
            # Extract filename from URL
            filename = storage_url.split('/')[-1]
            if '?' in filename:
                filename = filename.split('?')[0]
            if not filename.endswith('.xml'):
                filename = "downloaded_file.xml"
            
            files_data.append({
                'filename': filename,
                'content': response.content
            })
        
        else:
            # For folder links
            if 'drive.google.com' in storage_url and '/folders/' in storage_url:
                files_data = get_google_drive_folder_files(storage_url)
            else:
                frappe.throw(
                    "Folder URL processing is currently limited. Supported methods:\n"
                    "1. Use local folder path\n"
                    "2. Provide direct XML file download links\n"
                    "3. For Google Drive: Share individual files and use their direct download links"
                )
    
    except requests.RequestException as e:
        frappe.throw(f"Error accessing cloud storage: {str(e)}")
    except Exception as e:
        frappe.throw(f"Error processing cloud storage URL: {str(e)}")
    
    return files_data

def get_camt_files_from_folder(folder_path):
    """Get all XML files from the specified folder (local or cloud)"""
    if is_cloud_storage_url(folder_path):
        # Handle cloud storage URL
        return get_files_from_cloud_storage(folder_path)
    
    # Handle local folder path
    if not folder_path or not os.path.exists(folder_path):
        return []
    
    # Get all .xml files from local folder
    xml_pattern = os.path.join(folder_path, "*.xml")
    xml_files = glob.glob(xml_pattern)
    
    # Convert to consistent format (local files need to be read)
    files_data = []
    for file_path in sorted(xml_files):
        with open(file_path, 'rb') as f:
            files_data.append({
                'filename': os.path.basename(file_path),
                'content': f.read(),
                'local_path': file_path
            })
    
    return files_data

@frappe.whitelist()
def process_folder_files(folder_path, from_date=None, to_date=None, bank_account=None):
    """Process multiple CAMT.053 files from a folder/URL with bank account validation"""
    if not folder_path:
        frappe.throw("Please provide a folder path or cloud storage URL.")
    
    # Check if it's a cloud storage URL or local path
    is_cloud = is_cloud_storage_url(folder_path)
    
    if not is_cloud and not os.path.exists(folder_path):
        frappe.throw(f"Folder path does not exist: {folder_path}")
    
    # Get all XML files from folder or cloud storage
    xml_files_data = get_camt_files_from_folder(folder_path)
    
    if not xml_files_data:
        if is_cloud:
            frappe.throw("No XML files found at the specified cloud storage URL.")
        else:
            frappe.throw("No XML files found in the specified folder.")
    
    # Get reference IBAN for validation
    reference_iban = None
    detected_bank_account = bank_account
    
    # If bank account is provided, get its IBAN
    if bank_account:
        reference_iban = get_bank_account_iban(bank_account)
    
    # Process files
    all_transactions = []
    processing_summary = {
        "total_files": len(xml_files_data),
        "processed_files": 0,
        "skipped_files": [],
        "skip_reasons": {}
    }
    
    for i, file_data in enumerate(xml_files_data):
        filename = file_data['filename']
        file_content = file_data['content']
        
        try:
            # If this is the first file and no bank account specified, extract bank account
            if i == 0 and not detected_bank_account:
                # Process first file to get bank account
                file_result = process_single_file_content(file_content, from_date, to_date)
                if file_result.get("bank_account"):
                    detected_bank_account = file_result["bank_account"]
                    reference_iban = get_bank_account_iban(detected_bank_account)
            
            # Validate file bank account
            if not validate_file_bank_account(file_content, reference_iban):
                processing_summary["skipped_files"].append(filename)
                processing_summary["skip_reasons"][filename] = "Different bank account or invalid format"
                continue
            
            # Process the file
            file_result = process_single_file_content(file_content, from_date, to_date)
            if file_result.get("transactions"):
                all_transactions.extend(file_result["transactions"])
                processing_summary["processed_files"] += 1
        
        except Exception as e:
            processing_summary["skipped_files"].append(filename)
            processing_summary["skip_reasons"][filename] = f"Processing error: {str(e)}"
    
    # Remove duplicates based on reference number
    unique_transactions = []
    seen_references = set()
    
    for transaction in all_transactions:
        ref_key = f"{transaction.get('reference_number', '')}_{transaction.get('date', '')}_{transaction.get('deposit', 0)}_{transaction.get('withdrawal', 0)}"
        if ref_key not in seen_references:
            seen_references.add(ref_key)
            unique_transactions.append(transaction)
    
    return {
        "transactions": unique_transactions,
        "bank_account": detected_bank_account,
        "processing_summary": processing_summary
    }

def process_single_file_content(file_content, from_date=None, to_date=None):
    """Process single file content - extracted from process_camt53_file for reuse"""
    from_date_obj = datetime.strptime(from_date, '%Y-%m-%d').date() if from_date else None
    to_date_obj = datetime.strptime(to_date, '%Y-%m-%d').date() if to_date else None
    
    # Detect CAMT version from file content
    namespaces = {'camt': 'urn:iso:std:iso:20022:tech:xsd:camt.053.001.02'}  # default
    file_content_str = str(file_content)
    if 'camt.053.001.10' in file_content_str:
        namespaces = {'camt': 'urn:iso:std:iso:20022:tech:xsd:camt.053.001.10'}
    elif 'camt.053.001.04' in file_content_str:
        namespaces = {'camt': 'urn:iso:std:iso:20022:tech:xsd:camt.053.001.04'}  # UBS uses this format
    elif 'camt.053.001.08' in file_content_str:
        namespaces = {'camt': 'urn:iso:std:iso:20022:tech:xsd:camt.053.001.08'}
    
    root = ET.fromstring(file_content)
    transactions_for_preview = []
    main_bank_account_iban = ""
    
    for stmt_element in root.findall('.//camt:BkToCstmrStmt/camt:Stmt', namespaces):
        if not main_bank_account_iban:
            main_bank_account_iban = stmt_element.findtext('./camt:Acct/camt:Id/camt:IBAN', default="", namespaces=namespaces)
        
        for ntry_element in stmt_element.findall('./camt:Ntry', namespaces):
            # Check status - handle both formats
            status_cd_element = ntry_element.find('./camt:Sts/camt:Cd', namespaces)
            status_direct_element = ntry_element.find('./camt:Sts', namespaces)
            
            status = None
            if status_cd_element is not None:
                status = status_cd_element.text
            elif status_direct_element is not None:
                status = status_direct_element.text
            
            if status not in ['BOOK', 'PDNG']:
                continue
            
            # Try different date formats: DtTm (Wise format) or Dt (UBS format)
            booking_date_str = ""
            booking_date = None
            
            # First try DtTm format (Wise: 2023-12-07T13:33:28.811475+01:00)
            date_time_element = ntry_element.find('./camt:BookgDt/camt:DtTm', namespaces)
            if date_time_element is not None and date_time_element.text:
                booking_date_str = date_time_element.text
                # Parse full datetime format and extract date
                booking_date = datetime.fromisoformat(booking_date_str.replace('Z', '+00:00')).date()
            else:
                # Fallback to simple Dt format (UBS: 2024-08-28)
                booking_date_str = ntry_element.findtext('./camt:BookgDt/camt:Dt', default="", namespaces=namespaces)
                if booking_date_str:
                    booking_date = datetime.strptime(booking_date_str, '%Y-%m-%d').date()
            
            # Apply date filtering if booking date was found
            if booking_date:
                if from_date_obj and booking_date < from_date_obj:
                    continue
                if to_date_obj and booking_date > to_date_obj:
                    continue
            
            cdt_dbt_indicator = ntry_element.findtext('./camt:CdtDbtInd', default="", namespaces=namespaces)
            amount_value = float(ntry_element.findtext('./camt:Amt', default=0, namespaces=namespaces))
            currency = ntry_element.findtext('./camt:Amt', namespaces=namespaces)
            currency = ntry_element.find('./camt:Amt', namespaces).get('Ccy') if ntry_element.find('./camt:Amt', namespaces) is not None else ""
            
            description = ntry_element.findtext('./camt:AddtlNtryInf', default="", namespaces=namespaces)
            
            # Try different reference number locations for different bank formats
            # Priority: UBS AcctSvcrRef → UBS RmtInf/Ref → Wise BkTxCd format
            reference_number = ntry_element.findtext('./camt:AcctSvcrRef', default="", namespaces=namespaces)  # UBS format first
            
            # UBS QRR reference from remittance information (try second)
            if not reference_number:
                # Look for structured remittance info with QRR reference (UBS format)
                # Try multiple paths to find transaction details
                tx_dtls_paths = [
                    './camt:NtryDtls/camt:TxDtls',
                    './camt:TxDtls'
                ]
                
                for tx_path in tx_dtls_paths:
                    tx_dtls = ntry_element.find(tx_path, namespaces)
                    if tx_dtls is not None:
                        rmt_inf = tx_dtls.find('./camt:RmtInf', namespaces)
                        if rmt_inf is not None:
                            strd = rmt_inf.find('./camt:Strd', namespaces)
                            if strd is not None:
                                cdtr_ref_inf = strd.find('./camt:CdtrRefInf', namespaces)
                                if cdtr_ref_inf is not None:
                                    # Try to get reference regardless of type first
                                    qrr_ref = cdtr_ref_inf.findtext('./camt:Ref', namespaces=namespaces)
                                    if qrr_ref:
                                        # Check if this is a QRR reference for formatting
                                        tp_element = cdtr_ref_inf.find('./camt:Tp/camt:CdOrPrtry/camt:Prtry', namespaces)
                                        if tp_element is not None and tp_element.text == 'QRR':
                                            reference_number = format_qrr_reference(qrr_ref)
                                        else:
                                            # Use reference as-is if not QRR type or type unknown
                                            reference_number = qrr_ref
                                        break
            
            # Also try direct RmtInf path (some UBS formats may have this structure)
            if not reference_number:
                rmt_inf = ntry_element.find('./camt:RmtInf', namespaces)
                if rmt_inf is not None:
                    strd = rmt_inf.find('./camt:Strd', namespaces)
                    if strd is not None:
                        cdtr_ref_inf = strd.find('./camt:CdtrRefInf', namespaces)
                        if cdtr_ref_inf is not None:
                            qrr_ref = cdtr_ref_inf.findtext('./camt:Ref', namespaces=namespaces)
                            if qrr_ref:
                                tp_element = cdtr_ref_inf.find('./camt:Tp/camt:CdOrPrtry/camt:Prtry', namespaces)
                                if tp_element is not None and tp_element.text == 'QRR':
                                    reference_number = format_qrr_reference(qrr_ref)
                                else:
                                    reference_number = qrr_ref
            
            # Fallback to Wise format
            if not reference_number:
                reference_number = ntry_element.findtext('./camt:BkTxCd/camt:Prtry/camt:Cd', default="", namespaces=namespaces)  # Wise format

            existing_transaction = find_existing_transaction(reference_number)
            existing_transaction_name = existing_transaction if existing_transaction else None
            
            final_amount_value = amount_value
            reversal_indicator = ntry_element.findtext('./camt:RvslInd', default="", namespaces=namespaces)
            if reversal_indicator == "true":
                if cdt_dbt_indicator == 'CRDT':
                    final_amount_value = -amount_value
                elif cdt_dbt_indicator == 'DBIT':
                    final_amount_value = amount_value

            # Skip transactions with zero amount
            if final_amount_value == 0:
                continue

            current_row_data = {
                "date": booking_date.strftime('%Y-%m-%d') if booking_date else "",
                "description": description,
                "reference_number": reference_number,
                "currency": currency,
                "bank_transaction": existing_transaction_name,
                "deposit": None,
                "withdrawal": None
            }
            
            if cdt_dbt_indicator == 'DBIT':
                current_row_data["withdrawal"] = final_amount_value
            elif cdt_dbt_indicator == 'CRDT':
                current_row_data["deposit"] = final_amount_value

            # NEW: Extract structured party information first (UBS format)
            party_info = extract_party_from_structured_data(ntry_element, namespaces)
            found_party = find_party_from_structured_data(party_info, cdt_dbt_indicator)
            
            # Fallback to description-based matching if structured data didn't work
            if not found_party:
                found_party = find_party_from_details(description)
            
            if found_party:
                current_row_data["party_type"] = found_party["party_type"]
                current_row_data["party"] = found_party["party"]
                
            # Add debug info for structured data
            current_row_data["debug_party_info"] = party_info
            
            # Add debug info for reference number source
            debug_ref_info = {"reference_number": reference_number}
            if ntry_element.findtext('./camt:AcctSvcrRef', default="", namespaces=namespaces):
                debug_ref_info["ref_source"] = "UBS_AcctSvcrRef"
            elif reference_number and reference_number != ntry_element.findtext('./camt:BkTxCd/camt:Prtry/camt:Cd', default="", namespaces=namespaces):
                debug_ref_info["ref_source"] = "UBS_RmtInf"
            else:
                debug_ref_info["ref_source"] = "Wise_BkTxCd"
            current_row_data["debug_ref_info"] = debug_ref_info
            
            transactions_for_preview.append(current_row_data)
    
    erpnext_bank_account_name = find_bank_account_by_iban(main_bank_account_iban)
    
    # DEBUG: Log IBAN extraction and lookup
    frappe.log_error(f"Extracted IBAN from file: '{main_bank_account_iban}' -> Found Bank Account: '{erpnext_bank_account_name}'", "Bank Account Detection Debug")
    
    return {
        "transactions": transactions_for_preview,
        "bank_account": erpnext_bank_account_name
    }

def process_zip_file(file_content, from_date=None, to_date=None):
    """Process ZIP file containing multiple CAMT.053 XML files"""
    try:
        # Create a file-like object from the content
        zip_file_obj = io.BytesIO(file_content)
        
        # Extract XML files from ZIP
        files_data = []
        with zipfile.ZipFile(zip_file_obj, 'r') as zip_file:
            for filename in zip_file.namelist():
                if filename.lower().endswith('.xml') and not filename.startswith('__MACOSX/'):
                    try:
                        xml_content = zip_file.read(filename)
                        files_data.append({
                            'filename': os.path.basename(filename),
                            'content': xml_content
                        })
                    except Exception as e:
                        frappe.log_error(f"Error extracting {filename} from ZIP: {str(e)}", "ZIP Processing Error")
                        continue
        
        if not files_data:
            frappe.throw("No XML files found in the uploaded ZIP file.")
        
        # Process like folder processing
        all_transactions = []
        detected_bank_account = None
        reference_iban = None
        
        processing_summary = {
            "total_files": len(files_data),
            "processed_files": 0,
            "skipped_files": [],
            "skip_reasons": {}
        }
        
        for i, file_data in enumerate(files_data):
            filename = file_data['filename']
            file_content = file_data['content']
            
            try:
                # If this is the first file, extract bank account
                if i == 0:
                    file_result = process_single_file_content(file_content, from_date, to_date)
                    if file_result.get("bank_account"):
                        detected_bank_account = file_result["bank_account"]
                        reference_iban = get_bank_account_iban(detected_bank_account)
                
                # Validate file bank account if we have a reference
                if reference_iban and not validate_file_bank_account(file_content, reference_iban):
                    processing_summary["skipped_files"].append(filename)
                    processing_summary["skip_reasons"][filename] = "Different bank account or invalid format"
                    continue
                
                # Process the file
                file_result = process_single_file_content(file_content, from_date, to_date)
                if file_result.get("transactions"):
                    all_transactions.extend(file_result["transactions"])
                    processing_summary["processed_files"] += 1
            
            except Exception as e:
                processing_summary["skipped_files"].append(filename)
                processing_summary["skip_reasons"][filename] = f"Processing error: {str(e)}"
        
        # Remove duplicates
        unique_transactions = []
        seen_references = set()
        
        for transaction in all_transactions:
            ref_key = f"{transaction.get('reference_number', '')}_{transaction.get('date', '')}_{transaction.get('deposit', 0)}_{transaction.get('withdrawal', 0)}"
            if ref_key not in seen_references:
                seen_references.add(ref_key)
                unique_transactions.append(transaction)
        
        return {
            "transactions": unique_transactions,
            "bank_account": detected_bank_account,
            "processing_summary": processing_summary
        }
    
    except zipfile.BadZipFile:
        frappe.throw("The uploaded file is not a valid ZIP file.")
    except Exception as e:
        frappe.throw(f"Error processing ZIP file: {str(e)}")

@frappe.whitelist()
def process_camt53_file(file_url, from_date=None, to_date=None):
    """Process single CAMT.053 file or ZIP file containing multiple files"""
    if not file_url:
        frappe.throw("Please attach a CAMT.053 XML file or ZIP file first.")
    
    try:
        file_doc = frappe.get_doc("File", {"file_url": file_url})
        file_content = file_doc.get_content()
        
        # Check if it's a ZIP file
        if file_doc.file_name and file_doc.file_name.lower().endswith('.zip'):
            # Process as ZIP file containing multiple XML files
            return process_zip_file(file_content, from_date, to_date)
        else:
            # Process as single XML file
            return process_single_file_content(file_content, from_date, to_date)
    
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "CAMT.053 Import Error")
        frappe.throw(f"An error occurred during processing: {str(e)}")

@frappe.whitelist()
def create_bank_transactions(transactions, bank_account):
    import json
    transactions_data = json.loads(transactions)
    if not bank_account:
        frappe.throw("Bank Account is not set. Please preview the file first.")

    created_docs = []
    skipped_docs = []
    for tx in transactions_data:
        if tx.get("bank_transaction"):
            skipped_docs.append(tx.get("bank_transaction"))
            continue
        new_transaction = frappe.new_doc("Bank Transaction")
        new_transaction.date = tx.get("date")
        new_transaction.bank_account = bank_account
        new_transaction.deposit = tx.get("deposit")
        new_transaction.withdrawal = tx.get("withdrawal")
        new_transaction.description = tx.get("description")
        new_transaction.reference_number = tx.get("reference_number")
        if tx.get("party_type") and tx.get("party"):
            new_transaction.party_type = tx.get("party_type")
            new_transaction.party = tx.get("party")
        new_transaction.insert(ignore_permissions=True)
        new_transaction.submit()
        created_docs.append({
            "reference_number": tx.get("reference_number"),
            "doc_name": new_transaction.name
        })

    message = f"Successfully created and submitted {len(created_docs)} Bank Transaction(s)."
    return {
        "message": message,
        "created_docs": created_docs,
        "skipped_docs": skipped_docs
    }