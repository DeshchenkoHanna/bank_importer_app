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

def find_party_from_details(description):
    
    customers_with_bank_name = frappe.get_all("Customer", fields=["name", "custom_name_in_bank"], filters={"custom_name_in_bank": ["!=", ""], "disabled": 0})
    for customer in customers_with_bank_name:
        if customer.custom_name_in_bank.lower() in description.lower():
            return {"party_type": "Customer", "party": customer.name}

    suppliers_with_bank_name = frappe.get_all("Supplier", fields=["name", "custom_name_in_bank"], filters={"custom_name_in_bank": ["!=", ""], "disabled": 0})
    for supplier in suppliers_with_bank_name:
        if supplier.custom_name_in_bank.lower() in description.lower():
            return {"party_type": "Supplier", "party": supplier.name}
        
    best_match = None
    highest_score = 0.9
    parties_to_check = []
    customers = frappe.get_all("Customer", fields=["name", "customer_name"], filters={"disabled": 0})
    for c in customers:
        parties_to_check.append({"name": c.name, "search_name": c.customer_name, "type": "Customer"})
    suppliers = frappe.get_all("Supplier", fields=["name", "supplier_name"], filters={"disabled": 0})
    for s in suppliers:
        parties_to_check.append({"name": s.name, "search_name": s.supplier_name, "type": "Supplier"})
    for party_info in parties_to_check:
        if not party_info["search_name"]:
            continue
        if party_info["search_name"].lower() in description.lower():
            return {"party_type": party_info["type"], "party": party_info["name"]}
        for word in description.split():
            score = SequenceMatcher(None, party_info["search_name"].lower(), word.lower()).ratio()
            if score > highest_score:
                highest_score = score
                best_match = {"party_type": party_info["type"], "party": party_info["name"]}
    if best_match:
        return best_match
    return None

def find_existing_transaction(reference_number):
    """Checks if a Bank Transaction with the same reference number already exists."""
    if not reference_number:
        return None
    return frappe.db.get_value("Bank Transaction", {"reference_number": reference_number, "docstatus": 1}, "name")

@frappe.whitelist()
def process_camt53_file(file_url, from_date=None, to_date=None):
    if not file_url:
        frappe.throw("Please attach a CAMT.053 XML file first.")
    try:
        file_doc = frappe.get_doc("File", {"file_url": file_url})
        file_content = file_doc.get_content()
        from_date_obj = datetime.strptime(from_date, '%Y-%m-%d').date() if from_date else None
        to_date_obj = datetime.strptime(to_date, '%Y-%m-%d').date() if to_date else None
        namespaces = {'camt': 'urn:iso:std:iso:20022:tech:xsd:camt.053.001.02'}
        if 'camt.053.001.10' in str(file_content):
             namespaces = {'camt': 'urn:iso:std:iso:20022:tech:xsd:camt.053.001.10'}
        root = ET.fromstring(file_content)
        transactions_for_preview = []
        main_bank_account_iban = ""
        for stmt_element in root.findall('.//camt:BkToCstmrStmt/camt:Stmt', namespaces):
            if not main_bank_account_iban:
                main_bank_account_iban = stmt_element.findtext('./camt:Acct/camt:Id/camt:IBAN', default="", namespaces=namespaces)
            for ntry_element in stmt_element.findall('./camt:Ntry', namespaces):
                status_cd_element = ntry_element.find('./camt:Sts/camt:Cd', namespaces)
                if status_cd_element is None or status_cd_element.text != 'BOOK':
                    continue
                date_time_element = ntry_element.find('./camt:BookgDt/camt:DtTm', namespaces)
                if date_time_element is None:
                    date_time_element = ntry_element.find('./camt:ValDt/camt:DtTm', namespaces)
                if date_time_element is None or not date_time_element.text:
                    continue
                transaction_date = datetime.fromisoformat(date_time_element.text).date()
                if from_date_obj and transaction_date < from_date_obj:
                    continue
                if to_date_obj and transaction_date > to_date_obj:
                    continue

                description = ntry_element.findtext('./camt:AddtlNtryInf', default="", namespaces=namespaces)
                reference_number = ntry_element.findtext('./camt:BkTxCd/camt:Prtry/camt:Cd', default="", namespaces=namespaces)

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
                found_party = find_party_from_details(description)
                if found_party:
                    current_row_data["party_type"] = found_party["party_type"]
                    current_row_data["party"] = found_party["party"]
                transactions_for_preview.append(current_row_data)
        erpnext_bank_account_name = frappe.db.get_value("Bank Account", {"iban": main_bank_account_iban}, "name")
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