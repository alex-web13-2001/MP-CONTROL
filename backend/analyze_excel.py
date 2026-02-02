import pandas as pd
import sys

FILE_PATH = "supplier_report.xlsx"

def main():
    try:
        # Load Excel
        # Assuming header is on row 0 or 1. Usually reports have some meta rows.
        # User screenshot shows header on row 1 (0-indexed) or 2.
        # Let's try reading with no header first to inspect.
        df_raw = pd.read_excel(FILE_PATH, header=None, nrows=10)
        print("--- Excel Preview (First 5 rows) ---")
        print(df_raw.head())
        
        # "Отчёт по данным..." is row 0.
        # "Бренд | Предмет ..." is row 1.
        df = pd.read_excel(FILE_PATH, header=1)
        
        # Debug: Print columns
        print("Columns found at header=1:")
        print(df.columns.tolist())
        
        # Find column for Vendor Code
        # "Артикул продавца" might be "Артикул продавца" or similar
        vendor_col = next((c for c in df.columns if 'Артикул' in str(c) and 'прод' in str(c)), None)
        if not vendor_col:
             # Try finding just 'Артикул' but check it is not WB articul
             vendor_col = next((c for c in df.columns if 'Артикул' in str(c) and 'WB' not in str(c)), None)

        # Find Qty Col: 'Выкупили' (under 'Выкупленные товары' merged cell?)
        # Pandas handles merged cells by putting the top header in key sometimes? Or Unnamed?
        # Looking at columns dump from previous run: 'Выкупленные товары', 'Unnamed: 14'
        # Likely: 'Выкупленные товары' is the merged header. The sub-headers might be in row 2?
        # Wait, if row 1 is header, then 'Выкупленные товары' might be the name for the column 'Выкупили, шт'?
        # Let's check if there is a row 2 used for subheaders.
        # If 'Выкупленные товары' is column 13.
        # 'Unnamed: 14' is 'К перечислению' probably?
        
        # Let's inspect first few data rows to guess columns by content.
        print("\nFirst Data Row (row 0 of df):")
        print(df.iloc[0])
        
        # Heuristic search
        qty_col = 'Выкупили, шт' 
        sales_col = 'Сумма заказов - нет, это заказано. Нужно реализовал.' 
        # Check if we have 'Выкупили, шт' directly.
        
        # If headers are split across rows 1 and 2, pandas `header=[1]` might be insufficient.
        # But let's see the columns from header=1 first.

        
        # Explicit Mapping based on previous run output
        qty_col = 'Выкупили, шт.'  # Note the dot
        sales_net_col = 'Сумма заказов минус комиссия WB, руб.'
        payout_col = 'К перечислению за товар, руб.'
        vendor_col = 'Артикул продавца'

        print(f"\nUsing Columns:\nQty: {qty_col}\nSalesNet: {sales_net_col}\nPayout: {payout_col}")
        
        # Helper to safely sum
        def get_sum(col):
            if col in df.columns:
                return df[col].sum()
            return 0
            
        target_code = "B_NORM15"
        # Normalize vendor code
        df[vendor_col] = df[vendor_col].astype(str).str.strip()
        subset = df[df[vendor_col] == target_code]
        
        total_qty = subset[qty_col].sum() if qty_col in subset.columns else 0
        total_payout = subset[payout_col].sum() if payout_col in subset.columns else 0
        total_sales_net = subset[sales_net_col].sum() if sales_net_col in subset.columns else 0
        
        print(f"\n--- B_NORM15 Excel Summary ---")
        print(f"Rows: {len(subset)}")
        print(f"Qty Sold ({qty_col}): {total_qty}")
        print(f"Sales Net ({sales_net_col}): {total_sales_net}")
        print(f"Payout ({payout_col}): {total_payout}")
        
        print(f"\n--- TOTAL REPORT SUMMARY ---")
        print(f"Total Qty: {df[qty_col].sum() if qty_col in df.columns else 0}")
        print(f"Total Payout: {df[payout_col].sum() if payout_col in df.columns else 0}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
