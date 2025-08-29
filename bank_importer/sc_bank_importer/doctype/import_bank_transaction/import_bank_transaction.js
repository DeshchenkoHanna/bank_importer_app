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

        // Set initial field interaction state
        toggle_file_field_based_on_folder(frm);

        frm.add_custom_button(__('Preview Transactions'), function() {
            // Smart logic: choose between folder or file processing
            if (frm.doc.camt53_folder && frm.doc.camt53_folder.trim()) {
                // Process folder
                process_folder_files(frm);
            } else if (frm.doc.camt53_file) {
                // Process single file
                process_single_file(frm);
            } else {
                // Neither is filled
                frappe.msgprint({
                    title: __('Error'),
                    indicator: 'red',
                    message: __('Please attach a CAMT.053 XML file OR enter a folder path/cloud storage URL.')
                });
                return;
            }
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
    },

    // Field interaction handler
    camt53_folder: function(frm) {
        toggle_file_field_based_on_folder(frm);
    }
});

function toggle_file_field_based_on_folder(frm) {
    if (frm.doc.camt53_folder && frm.doc.camt53_folder.trim()) {
        // Folder/URL has content - disable file field
        frm.set_df_property('camt53_file', 'read_only', 1);
        frm.set_df_property('camt53_file', 'description', 'File upload disabled when folder path or cloud storage URL is specified');
        
        // Optionally clear the file field
        if (frm.doc.camt53_file) {
            frm.set_value('camt53_file', '');
        }
    } else {
        // Folder/URL is empty - enable file field
        frm.set_df_property('camt53_file', 'read_only', 0);
        frm.set_df_property('camt53_file', 'description', '');
    }
}

function process_single_file(frm) {
    if (frm.doc.from_date && frm.doc.to_date && frm.doc.from_date > frm.doc.to_date) {
        frappe.msgprint({
            title: __('Error'),
            indicator: 'red',
            message: __('From Date cannot be after To Date.')
        });
        return;
    }

    frappe.show_alert({ message: __('Processing single file...'), indicator: 'blue' }, 5);

    frappe.call({
        method: 'bank_importer.sc_bank_importer.doctype.import_bank_transaction.import_bank_transaction.process_camt53_file',
        args: {
            file_url: frm.doc.camt53_file,
            from_date: frm.doc.from_date,
            to_date: frm.doc.to_date
        },
        callback: function(r) {
            if (r.message) {
                populate_transaction_table(frm, r.message);
                
                // Check if it was a ZIP file with processing summary
                if (r.message.processing_summary) {
                    let summary = r.message.processing_summary;
                    let summary_message = `ZIP file processed: ${summary.processed_files} of ${summary.total_files} files successfully.`;
                    
                    if (summary.skipped_files.length > 0) {
                        summary_message += `<br><br><strong>Skipped files:</strong><br>`;
                        summary.skipped_files.forEach(function(filename) {
                            let reason = summary.skip_reasons[filename] || 'Unknown reason';
                            summary_message += `• ${filename}: ${reason}<br>`;
                        });
                    }
                    
                    frappe.msgprint({
                        title: __('ZIP File Processing Complete'),
                        indicator: summary.skipped_files.length > 0 ? 'orange' : 'green',
                        message: summary_message
                    });
                } else {
                    frappe.msgprint({
                        title: __('Success'),
                        indicator: 'green',
                        message: __('Single file processed successfully. Review the transactions below.')
                    });
                }
            }
        }
    });
}

function process_folder_files(frm) {
    if (frm.doc.from_date && frm.doc.to_date && frm.doc.from_date > frm.doc.to_date) {
        frappe.msgprint({
            title: __('Error'),
            indicator: 'red',
            message: __('From Date cannot be after To Date.')
        });
        return;
    }

    // Determine if it's a URL or local path for better user feedback
    let is_url = frm.doc.camt53_folder.includes('http');
    let processing_message = is_url ? 'Processing cloud storage files...' : 'Processing folder files...';
    
    frappe.show_alert({ message: __(processing_message), indicator: 'blue' }, 10);

    frappe.call({
        method: 'bank_importer.sc_bank_importer.doctype.import_bank_transaction.import_bank_transaction.process_folder_files',
        args: {
            folder_path: frm.doc.camt53_folder,
            from_date: frm.doc.from_date,
            to_date: frm.doc.to_date,
            bank_account: frm.doc.bank_account
        },
        callback: function(r) {
            if (r.message) {
                populate_transaction_table(frm, r.message);
                
                // Show processing summary
                let summary = r.message.processing_summary;
                let summary_message = `Processed ${summary.processed_files} of ${summary.total_files} files successfully.`;
                
                if (summary.skipped_files.length > 0) {
                    summary_message += `<br><br><strong>Skipped files:</strong><br>`;
                    summary.skipped_files.forEach(function(filename) {
                        let reason = summary.skip_reasons[filename] || 'Unknown reason';
                        summary_message += `• ${filename}: ${reason}<br>`;
                    });
                }
                
                frappe.msgprint({
                    title: __('Folder Processing Complete'),
                    indicator: summary.skipped_files.length > 0 ? 'orange' : 'green',
                    message: summary_message
                });
            }
        }
    });
}

function populate_transaction_table(frm, response_data) {
    // Set bank account if detected
    if (response_data.bank_account) {
        frm.set_value('bank_account', response_data.bank_account);
    }
    
    // Clear and populate transaction table
    frm.clear_table('transactions');
    
    response_data.transactions.forEach(function(d) {
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
}



