# Copyright (c) 2025, Swiss Cluster and contributors
# For license information, please see license.txt

# import frappe
import frappe
import xml.etree.ElementTree as ET
from datetime import datetime
from difflib import SequenceMatcher

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

@frappe.whitelist()
def process_camt53_file(file_url, from_date=None, to_date=None):
    if not file_url:
        frappe.throw("Please attach a CAMT.053 XML file first.")
    try:
        file_doc = frappe.get_doc("File", {"file_url": file_url})
        file_content = file_doc.get_content()
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
                    
                if status != 'BOOK':
                    continue
                # Try different date formats: DtTm (version 001.10) or Dt (version 001.04)
                date_time_element = ntry_element.find('./camt:BookgDt/camt:DtTm', namespaces)
                if date_time_element is None:
                    date_time_element = ntry_element.find('./camt:ValDt/camt:DtTm', namespaces)
                if date_time_element is None:
                    # Try simple Dt format (UBS 001.04)
                    date_time_element = ntry_element.find('./camt:BookgDt/camt:Dt', namespaces)
                if date_time_element is None:
                    date_time_element = ntry_element.find('./camt:ValDt/camt:Dt', namespaces)
                if date_time_element is None or not date_time_element.text:
                    continue
                
                # Parse date based on format
                date_text = date_time_element.text
                if 'T' in date_text:
                    # Full datetime format (001.10)
                    transaction_date = datetime.fromisoformat(date_text).date()
                else:
                    # Simple date format (001.04)
                    transaction_date = datetime.strptime(date_text, '%Y-%m-%d').date()
                if from_date_obj and transaction_date < from_date_obj:
                    continue
                if to_date_obj and transaction_date > to_date_obj:
                    continue

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

                current_row_data = {
                    "deposit": 0.0,
                    "withdrawal": 0.0,
                    "date": transaction_date.strftime('%Y-%m-%d'),
                    "description": description,
                    "reference_number": reference_number,
                    "bank_transaction": existing_transaction,
                }
                amt_tag = ntry_element.find('./camt:Amt', namespaces)
                original_amount_value = 0.0
                currency = ""
                if amt_tag is not None and amt_tag.text:
                    original_amount_value = float(amt_tag.text)
                    currency = amt_tag.get('Ccy', "")
                    current_row_data["currency"] = currency
                charges_amt_tag = ntry_element.find('./camt:Chrgs/camt:TtlChrgsAndTaxAmt', namespaces)
                charge_value = 0.0
                if charges_amt_tag is not None and charges_amt_tag.text:
                    charge_currency = charges_amt_tag.get('Ccy', "")
                    if charge_currency == currency:
                        charge_value = float(charges_amt_tag.text)
                final_amount_value = original_amount_value - charge_value
                cdt_dbt_indicator = ntry_element.findtext('./camt:CdtDbtInd', default="", namespaces=namespaces)
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
        return {
            "bank_account": erpnext_bank_account_name,
            "transactions": transactions_for_preview
        }
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