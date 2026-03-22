import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import os  # <--- เพิ่มบรรทัดนี้เข้าไปครับ!

st.set_page_config(page_title="Account Performance Tracker", layout="wide")

def load_multiple_sheets(file_name, start_date, end_date):
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = None

        # 1. พยายามหา Secrets ก่อน (ใช้ try เพื่อไม่ให้ระบบแจ้งเตือนถ้าไม่มี)
        try:
            if "gcp_service_account" in st.secrets:
                creds_info = st.secrets["gcp_service_account"]
                creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, scope)
        except:
            # ถ้าไม่มี secrets (รันในเครื่อง) ให้ข้ามไป ไม่ต้องทำอะไร
            pass

        # 2. ถ้าข้อ 1 ไม่สำเร็จ (แปลว่ารันในเครื่อง) ให้ใช้ไฟล์ JSON
        if creds is None:
            if os.path.exists("customerdb.json"):
                creds = ServiceAccountCredentials.from_json_keyfile_name("customerdb.json", scope)
            else:
                return None, "❌ ไม่พบกุญแจเชื่อมต่อ (ไฟล์ JSON หาย!)"

        client = gspread.authorize(creds)
        
        # --- ส่วนดึงข้อมูล (เหมือนเดิมที่คุณมี) ---
        spreadsheet = client.open(file_name)
        all_data = []
        current = start_date
        while current <= end_date:
            day_name = str(current.day)
            try:
                worksheet = spreadsheet.worksheet(day_name)
                raw_values = worksheet.get_all_values()
                if len(raw_values) > 1:
                    headers = [h.strip() for h in raw_values[0]]
                    temp_df = pd.DataFrame(raw_values[1:], columns=headers)
                    if all(col in temp_df.columns for col in ['Account', 'Valid Amount']):
                        temp_df = temp_df[['Account', 'Valid Amount']].copy()
                        temp_df['DataDate'] = pd.to_datetime(current)
                        all_data.append(temp_df)
            except:
                pass 
            current += timedelta(days=1)
            
        return (pd.concat(all_data, ignore_index=True), None) if all_data else (pd.DataFrame(), "ไม่พบข้อมูล")
    except Exception as e:
        return None, str(e)

# --- ส่วนควบคุมหลัก ---
st.write("### 🔍 ระบบตรวจสอบ Account รายวัน")
store_files = {"ADN": "ADN", "APB": "APB", "JYX": "JYX", "JDX": "JDX"}
selected_file = st.selectbox("📂 เลือกไฟล์ร้านค้า:", options=list(store_files.keys()))

st.divider()

yesterday = datetime.now().date() - timedelta(days=1)
today = datetime.now().date()

with st.form("search_form"):
    col1, col2 = st.columns(2)
    with col1:
        target_val_input = st.text_input("ระบุยอด Valid Amount ขั้นต่ำ:", value="1000")
        start_date = st.date_input("เริ่มจากวันที่:", value=yesterday)
    with col2:
        target_days_input = st.text_input("จำนวนวันที่ต้องทำได้ต่อเนื่อง:", value="2")
        end_date = st.date_input("ถึงวันที่:", value=today)
    
    submit_btn = st.form_submit_button("🚀 เริ่มการค้นหา", use_container_width=True)

if submit_btn:
    with st.spinner('กำลังประมวลผลข้อมูล...'):
        raw_df, error = load_multiple_sheets(selected_file, start_date, end_date)
    
    if error:
        st.warning(f"⚠️ {error}")
    elif not raw_df.empty:
        try:
            target_val = float(target_val_input.replace(',', ''))
            required_streak = int(target_days_input)
            
            raw_df['Valid Amount'] = pd.to_numeric(raw_df['Valid Amount'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
            
            # 1. กรองและคำนวณ Streak
            hit_df = raw_df[raw_df['Valid Amount'] >= target_val].copy()
            hit_df = hit_df.sort_values(['Account', 'DataDate'])
            
            hit_df['diff'] = hit_df.groupby('Account')['DataDate'].diff().dt.days
            hit_df['is_new_streak'] = (hit_df['diff'] != 1)
            hit_df['streak_group'] = hit_df.groupby('Account')['is_new_streak'].cumsum()
            hit_df['current_streak'] = hit_df.groupby(['Account', 'streak_group']).cumcount() + 1
            
            # 2. หา Account ที่ผ่านเกณฑ์
            success_accounts = hit_df[hit_df['current_streak'] >= required_streak]['Account'].unique()
            
            if len(success_accounts) > 0:
                # --- ส่วนแจ้งผลลัพธ์หลัก ---
                st.success(f"🎯 พบทั้งหมด {len(success_accounts)} Account ที่เข้าเงื่อนไข")
                
                # 1. แถบรายชื่อ Account (พับไว้)
                with st.expander("📋 คลิกเพื่อดูรายชื่อ Account ทั้งหมด", expanded=False):
                    account_only_df = pd.DataFrame({
                        "รายชื่อ Account": sorted(success_accounts)
                    })
                    st.dataframe(
                        account_only_df,
                        use_container_width=True,
                        hide_index=True
                    )
                
                # 2. แถบรายละเอียดรายวัน (พับไว้ และวางต่อกันทันทีไม่มีเส้นคั่น)
                with st.expander("📊 คลิกเพื่อดูรายละเอียดการทำยอดรายวัน (Valid Amount)", expanded=False):
                    final_res = hit_df[hit_df['Account'].isin(success_accounts)]
                    final_res = final_res[['Account', 'DataDate', 'Valid Amount']].sort_values(['Account', 'DataDate'])
                    final_res['DataDate'] = final_res['DataDate'].dt.strftime('%d/%m/%Y')
                    
                    st.dataframe(
                        final_res.style.format({'Valid Amount': '{:,.2f}'}),
                        use_container_width=True,
                        hide_index=True
                    )
            else:
                st.info(f"💡 ไม่พบ Account ที่ทำยอดได้ต่อเนื่อง {required_streak} วัน")
                
        except Exception as e:
            st.error(f"❌ เกิดข้อผิดพลาด: {str(e)}")