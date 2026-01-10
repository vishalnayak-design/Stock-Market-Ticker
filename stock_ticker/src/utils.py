import csv
import os
import logging
import xlsxwriter

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def save_to_csv(data_list, path, headers=None):
    """Saves a list of dictionaries to CSV."""
    try:
        if not data_list:
            return
            
        ensure_dir(os.path.dirname(path))
        
        # Determine headers if not provided (Handle Schema Evolution)
        if not headers:
            # Start with keys from the first row to preserve preferred order
            headers = list(data_list[0].keys())
            # Check other rows for new columns (e.g., Sector, Industry)
            current_keys = set(headers)
            for row in data_list[1:]:
                for k in row.keys():
                    if k not in current_keys:
                        headers.append(k)
                        current_keys.add(k)
            
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data_list)
            
        logging.info(f"Saved data to {path}")
    except Exception as e:
        logging.error(f"Failed to save {path}: {e}")

def read_csv_to_list(path):
    """Reads CSV into a list of dictionaries."""
    try:
        if not os.path.exists(path):
            return []
            
        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return list(reader)
    except Exception as e:
        logging.error(f"Failed to read {path}: {e}")
        return []

def save_to_excel(data_list, path):
    """Saves list of dicts to Excel using xlsxwriter with formatting."""
    try:
        if not data_list:
            return
            
        ensure_dir(os.path.dirname(path))
        
        workbook = xlsxwriter.Workbook(path)
        worksheet = workbook.add_worksheet("Picks")
        
        headers = list(data_list[0].keys())
        
        # Formats
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3', 'border': 1})
        green_fmt = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        red_fmt = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        
        # Write Headers
        for col_num, header in enumerate(headers):
            worksheet.write(0, col_num, header, header_fmt)
            
        # Write Data
        for row_num, row_data in enumerate(data_list, start=1):
            for col_num, key in enumerate(headers):
                val = row_data.get(key)
                worksheet.write(row_num, col_num, val)
                
        # Conditional Formatting (Manual Loop)
        # We find columns by name index
        try:
            score_col_idx = headers.index('Final_Score') if 'Final_Score' in headers else headers.index('Fund_Score')
            pe_col_idx = headers.index('PE_Ratio') if 'PE_Ratio' in headers else -1
            
            for row_num in range(1, len(data_list) + 1):
                # Score Color
                val = data_list[row_num-1].get(headers[score_col_idx], 0)
                if isinstance(val, (int, float)):
                    if val > 0.7:
                         worksheet.write(row_num, score_col_idx, val, green_fmt)
                    elif val < 0.4:
                         worksheet.write(row_num, score_col_idx, val, red_fmt)
                         
                # PE Color
                if pe_col_idx != -1:
                    pe_val = data_list[row_num-1].get('PE_Ratio', 0)
                    if isinstance(pe_val, (int, float)):
                         if pe_val < 25:
                             worksheet.write(row_num, pe_col_idx, pe_val, green_fmt)
                         elif pe_val > 50:
                             worksheet.write(row_num, pe_col_idx, pe_val, red_fmt)
                             
        except ValueError:
            pass # Column not found
            
        workbook.close()
        logging.info(f"Saved Excel to {path}")
    except Exception as e:
        logging.error(f"Failed to save Excel {path}: {e}")
