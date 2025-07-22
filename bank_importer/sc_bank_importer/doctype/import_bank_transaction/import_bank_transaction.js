// Copyright (c) 2025, Swiss Cluster and contributors
// For license information, please see license.txt

// frappe.ui.form.on("Import Bank Transaction", {
// 	refresh(frm) {

// 	},
// });
frappe.ui.form.on('Import Bank Transaction', {
    refresh: function(frm) {
        frm.clear_custom_buttons();

        frm.set_query("party_type", "transactions", function() {
            return {
                filters: {
                    name: ["in", Object.keys(frappe.boot.party_account_types)],
                },
            };
        });

        frm.add_custom_button(__('Preview Transactions'), function() {
            if (!frm.doc.camt53_file) {
                frappe.msgprint({
                    title: __('Error'),
                    indicator: 'red',
                    message: __('Please attach a CAMT.053 XML file first.')
                });
                return;
            }

            if (frm.doc.from_date && frm.doc.to_date && frm.doc.from_date > frm.doc.to_date) {
                frappe.msgprint({
                    title: __('Error'),
                    indicator: 'red',
                    message: __('From Date cannot be after To Date.')
                });
                return;
            }

            frappe.show_alert({ message: __('Processing...'), indicator: 'blue' }, 5);

            frappe.call({
                method: 'bank_importer.sc_bank_importer.doctype.import_bank_transaction.import_bank_transaction.process_camt53_file',
                args: {
                    file_url: frm.doc.camt53_file,
                    from_date: frm.doc.from_date,
                    to_date: frm.doc.to_date
                },
                callback: function(r) {
                    if (r.message) {
                        frm.set_value('bank_account', r.message.bank_account);
                        frm.clear_table('transactions');
                        r.message.transactions.forEach(function(d) {
                            var child = frm.add_child('transactions');
                            frappe.model.set_value(child.doctype, child.name, 'date', d.date);
                            frappe.model.set_value(child.doctype, child.name, 'deposit', d.deposit);
                            frappe.model.set_value(child.doctype, child.name, 'withdrawal', d.withdrawal);
                            frappe.model.set_value(child.doctype, child.name, 'description', d.description);
                            frappe.model.set_value(child.doctype, child.name, 'reference_number', d.reference_number);
                            frappe.model.set_value(child.doctype, child.name, 'currency', d.currency);
                            frappe.model.set_value(child.doctype, child.name, 'bank_transaction', d.bank_transaction);
                            if (d.party_type && d.party) {
                                frappe.model.set_value(child.doctype, child.name, 'party_type', d.party_type);
                                frappe.model.set_value(child.doctype, child.name, 'party', d.party);
                            }
                        });
                        frm.refresh_field('transactions');
                        // Automatically check rows that don't have an existing transaction
                        frm.doc.transactions.forEach(function(row) {
                            if (!row.bank_transaction) {
                                row.__checked = 1;
                            }
                        });
                        frm.fields_dict.transactions.grid.refresh();
                        frappe.msgprint({
                            title: __('Success'),
                            indicator: 'green',
                            message: __('Preview generated successfully. Review the transactions below.')
                        });
                    }
                }
            });
        });

        //if (frm.doc.transactions && frm.doc.transactions.length > 0) {
     //   if (frm.doc.transactions && frm.doc.transactions.some(row => !row.bank_transaction)) {     
            frm.add_custom_button(__('Create Bank Transactions'), function() {
                const selected_transactions = frm.doc.transactions.filter(d => d.__checked);

                if (selected_transactions.length === 0) {
                    frappe.msgprint(__('Please select at least one transaction to create.'));
                    return;
                }
                let invalid_rows = [];
                selected_transactions.forEach(function(row) {
                    if (!row.party) {
                        invalid_rows.push(row.idx);
                    }
                });

                if (invalid_rows.length > 0) {
                    frappe.msgprint({
                        title: __('Missing Party'),
                        indicator: 'red',
                        message: __('Cannot create transactions. Please assign a Party to row(s): ' + invalid_rows.join(', ') + ' or uncheck them.')
                    });
                    return;
                }

                frappe.show_alert({ message: __('Creating Documents...'), indicator: 'blue' }, 5);

                frappe.call({
                    method: 'bank_importer.sc_bank_importer.doctype.import_bank_transaction.import_bank_transaction.create_bank_transactions',
                    args: {
                        transactions: JSON.stringify(selected_transactions),
                        bank_account: frm.doc.bank_account
                    },
                    callback: function(r) {
                        if (r.message) {
                            let final_message = __(r.message.message);
                            if (r.message.skipped_docs && r.message.skipped_docs.length > 0) {
                                final_message += '<br><br>' + __('The following transactions were skipped as they already exist: ') + r.message.skipped_docs.join(', ');
                            }

                            frappe.msgprint({
                                title: __('Success'),
                                indicator: 'green',
                                message: final_message
                            });

                            // Update the grid with the new document links
                            r.message.created_docs.forEach(function(created_doc) {
                                let row = frm.doc.transactions.find(d => d.reference_number === created_doc.reference_number);
                                if (row) {
                                    frappe.model.set_value(row.doctype, row.name, 'bank_transaction', created_doc.doc_name);
                                }
                            });

                            // Uncheck all rows in the table
                            frm.doc.transactions.forEach(function(row) {
                                row.__checked = 0;
                            });
                            frm.refresh();
                            //frm.fields_dict.transactions.grid.refresh();
                           // frm.get_field('transactions').grid.toggle_reqd(false); // Remove the button by refreshing its display logic
                        }
                    }
                });
            });
       // }
    }
});



