import json
import requests
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter
from datetime import datetime, timedelta, timezone
import io
from flask import Flask, request, jsonify, send_file

# --- HELPER FUNCTIONS FROM YOUR ORIGINAL SCRIPT ---

def format_table(ws, start_row, start_col, num_rows, num_cols, align="center"):
    border = Border(left=Side(style="thin", color="000000"), right=Side(style="thin", color="000000"), top=Side(style="thin", color="000000"), bottom=Side(style="thin", color="000000"))
    if num_rows == 0: return # Do not format if there are no rows
    for r in range(start_row, start_row + num_rows):
        for c in range(start_col, start_col + num_cols):
            cell = ws.cell(row=r, column=c)
            cell.border = border
            if r == start_row: cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal=align, vertical='center', wrap_text=True)

def style_last_written_table(ws, title_text, bold_cols=None):
    border = Border(left=Side(style="thin", color="000000"), right=Side(style="thin", color="000000"), top=Side(style="thin", color="000000"), bottom=Side(style="thin", color="000000"))
    bold_font = Font(bold=True)
    start_header_row = 0
    for row in range(ws.max_row, 0, -1):
        if ws.cell(row=row, column=1).value == title_text:
            start_header_row = row + 2
            break
    if not start_header_row: return
    num_cols = 0
    while ws.cell(row=start_header_row, column=num_cols + 1).value: num_cols += 1
    for col in range(1, num_cols + 1):
        cell = ws.cell(row=start_header_row, column=col)
        cell.font = bold_font
        cell.border = border
    row = start_header_row + 1
    while ws.cell(row=row, column=1).value is not None and ws.cell(row=row, column=1).value != "":
        for col in range(1, num_cols + 1):
            cell = ws.cell(row=row, column=col)
            cell.border = border
            if ws.cell(row=row, column=1).value == "Grand Total": cell.font = bold_font
            if bold_cols and ws.cell(row=start_header_row, column=col).value in bold_cols: cell.font = Font(bold=True)
            if col >= 2: cell.alignment = Alignment(horizontal="center")
        row += 1

def autofit_columns(ws):
    for col in ws.columns:
        max_length = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            if cell.coordinate in ws.merged_cells: continue
            try:
                if cell.value: max_length = max(max_length, len(str(cell.value)))
            except: pass
        adjusted_width = max(max_length + 2, 10)
        ws.column_dimensions[col_letter].width = adjusted_width

def write_merged_title(ws, title, col_span=8, align="center"):
    ws.append([""] * col_span)
    row = ws.max_row
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=col_span)
    cell = ws.cell(row=row, column=1)
    cell.value = title
    cell.font = Font(bold=True)
    cell.alignment = Alignment(horizontal=align, vertical="center")

# --- FLASK APPLICATION SETUP ---
app = Flask(__name__)

@app.route('/')
def index():
    return app.send_static_file('index.html')

@app.route('/generate-report', methods=['POST'])
def generate_report():
    try:
        hk_tz = timezone(timedelta(hours=8))
        now_hkt = datetime.now(timezone.utc).astimezone(hk_tz)
        days_until_saturday = (5 - now_hkt.weekday() + 7) % 7
        this_saturday = (now_hkt + timedelta(days=days_until_saturday)).date()
        report_end = this_saturday
        report_begin = report_end - timedelta(days=7)

        data = request.json
        apiKey, boardId = data.get('apiKey'), data.get('boardId')
        apiUrl, headers = "https://api.monday.com/v2", {"Authorization": apiKey}

        all_items, cursor = [], None
        print("Fetching all items from Monday.com...")
        while True:
            query = f"""query {{ boards(ids: {boardId}) {{ items_page(limit:500) {{ cursor items {{ id name column_values {{ text column {{ title }} }} }} }} }} }}"""
            if cursor: query = f"""query {{ next_items_page(cursor: "{cursor}", limit:500) {{ cursor items {{ id name column_values {{ text column {{ title }} }} }} }} }}"""
            
            response = requests.post(apiUrl, json={'query': query}, headers=headers)
            response.raise_for_status()
            monday_data = response.json()
            if "errors" in monday_data: raise Exception(f"Monday.com API Error: {monday_data['errors']}")
            
            page_data = monday_data.get("data", {}).get("next_items_page") or monday_data.get("data", {}).get("boards", [{}])[0].get("items_page", {})
            current_items = page_data.get("items", [])
            if not current_items: break
            all_items.extend(current_items)
            cursor = page_data.get("cursor")
            if not cursor: break
        print(f"✅ Fetched {len(all_items)} total items.")

        rows, columns = [], []
        for item in all_items:
            row_data = {"Item ID": item["id"], "Item Name": item["name"]}
            for col in item.get("column_values", []):
                if col and col.get("column") and col["column"].get("title"):
                    col_title = col["column"]["title"]
                    row_data[col_title] = col["text"]
                    if col_title not in columns: columns.append(col_title)
            rows.append(row_data)
        
        ordered_columns = ["Item ID", "Item Name"] + [c for c in columns if c not in ["Item ID", "Item Name"]]
        df_data = pd.DataFrame(rows).reindex(columns=ordered_columns)

        print("Starting data analysis...")
        df_data["ReportBegin"], df_data["ReportEnd"] = report_begin, report_end
        for col in ["Deal creation date", "Close Date"]:
            if col in df_data.columns: df_data[col] = pd.to_datetime(df_data[col], errors="coerce").dt.date
        
        if 'Group Status' not in df_data.columns: df_data['Group Status'] = "Unknown"
        if 'Potential' not in df_data.columns: df_data['Potential'] = "Unknown"

        df_data["IsActiveNow"] = ((((df_data["Deal creation date"] < report_end) & (df_data["Group Status"] == "Active")) | ((df_data["Group Status"] != "Active") & (df_data["Close Date"] >= report_end)))).astype(int)
        df_data["IsActiveBeforeCutoff"] = (((df_data["Deal creation date"] < report_begin) & ((df_data["Group Status"] == "Active") | ((df_data["Group Status"] != "Active") & (df_data["Close Date"] >= report_begin))))).astype(int)
        df_data["AdditionAfterCutoff"] = ((df_data["Deal creation date"] >= report_begin) & (df_data["Deal creation date"] < report_end)).astype(int)
        df_data["RemovalAfterCutoff"] = (df_data["Close Date"].notna() & (df_data["Close Date"] >= report_begin) & (df_data["Close Date"] < report_end)).astype(int)
        df_data["IsHot"] = ((df_data["Potential"] == "Hot") & (df_data["IsActiveNow"] == 1)).astype(int)
        df_data["IsCold"] = ((df_data["Potential"] == "Cold") & (df_data["IsActiveNow"] == 1)).astype(int)

        print("Creating Excel workbook...")
        wb = Workbook()
        wb.remove(wb.active)
        ws_data = wb.create_sheet("Data")
        for r in dataframe_to_rows(df_data, index=False, header=True): ws_data.append(r)
        ws_summary = wb.create_sheet("Summary Report")

        # --- Build Full Summary Report ---
        
        # Reporting Period
        period_from, period_to = report_begin.strftime("%m/%d/%Y"), report_end.strftime("%m/%d/%Y")
        ws_summary.append([""] * 5 + ["Period:", "From", "To"]); [setattr(c, 'font', Font(bold=True)) for c in ws_summary[ws_summary.max_row][-3:]]
        ws_summary.append([""] * 5 + ["", period_from, period_to]); ws_summary.append([])
        
        metric_defs = {"This Week": "IsActiveNow", "Last Week": "IsActiveBeforeCutoff", "Addition (+)": "AdditionAfterCutoff", "Removal (-)": "RemovalAfterCutoff", "Hot": "IsHot", "Cold": "IsCold"}
        h1, v1, h2, v2 = [],[],[],[]
        for k, v in list(metric_defs.items())[:4]: h1.append(k); v1.append(df_data[v].sum())
        for k, v in list(metric_defs.items())[4:]: h2.append(k); v2.append(df_data[v].sum())
        ws_summary.append([""]*1+["Enquiries Movement"]); ws_summary.cell(row=ws_summary.max_row, column=2).font=Font(bold=True); ws_summary.append([]); ws_summary.append([""]*1+h1); s=ws_summary.max_row; ws_summary.append([""]*1+v1); format_table(ws_summary,s,2,2,len(h1)); ws_summary.append([])
        ws_summary.append([""]*1+["Enquiries by Potential"]); ws_summary.cell(row=ws_summary.max_row, column=2).font=Font(bold=True); ws_summary.append([]); ws_summary.append([""]*1+["Potential"]+h2); s=ws_summary.max_row; ws_summary.append([""]*1+[""]+v2); format_table(ws_summary,s,2,2,1+len(h2)); ws_summary.append([])

        # --- THIS IS THE SECTION WITH THE FIX ---
        for t in ["Addition", "Removal"]:
            df_s = df_data[df_data[f"{t}AfterCutoff"] == 1]
            cols = ["Dept", "Item Name", "Country/Region", "Salesperson", "Service", "Stage", "Referral Source Category", "Group Status"]
            # Correctly create a user-friendly title e.g. "Enquiries Added This Week"
            title = f"Enquiries {t.replace('ition', 'ed').replace('al', 'ed')} This Week"
            write_merged_title(ws_summary, title, align="left"); ws_summary.append([])
            ws_summary.append(cols); s = ws_summary.max_row
            if not df_s.empty:
                for _, r in df_s[cols].iterrows(): ws_summary.append(list(r))
            format_table(ws_summary, s, 1, 1+len(df_s), len(cols), align="left"); ws_summary.append([])
        # --- END OF FIX ---
        
        print("Formatting and saving file...")
        autofit_columns(ws_summary)
        
        file_stream = io.BytesIO()
        wb.save(file_stream)
        file_stream.seek(0)
        
        filename = f"Weekly_Enquiries_Report_{report_begin}_to_{report_end}.xlsx"
        
        return send_file(
            file_stream, as_attachment=True, download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

# --- RUN THE FLASK APPLICATION ---
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
