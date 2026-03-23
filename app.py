import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import os

# --- ตั้งค่าหน้าจอ ---
st.set_page_config(page_title="Dashboard ระบบจัดการข้อมูล", layout="wide")

# --- ฟังก์ชันดึงข้อมูล (รองรับทั้ง Local และ Cloud) ---
def get_gspread_client():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = None
        try:
            if "gcp_service_account" in st.secrets:
                creds_info = st.secrets["gcp_service_account"]
                creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, scope)
        except: pass
        if creds is None and os.path.exists("customerdb.json"):
            creds = ServiceAccountCredentials.from_json_keyfile_name("customerdb.json", scope)
        if creds: return gspread.authorize(creds), None
        return None, "ไม่พบกุญแจเชื่อมต่อ (customerdb.json)"
    except Exception as e: return None, str(e)

# --- ฟังก์ชันดึงข้อมูลดิบรายวัน (ปรับปรุงให้ยืดหยุ่นขึ้น) ---
@st.cache_data(ttl=600) # เก็บความจำไว้ 600 วินาที (10 นาที)
def fetch_raw_data_range(file_name, start_date, end_date):
    client, err = get_gspread_client()
    if err: return None, err
    try:
        spreadsheet = client.open(file_name)
        all_days_list = []
        current = start_date
        while current <= end_date:
            day_name = str(current.day)
            try:
                worksheet = spreadsheet.worksheet(day_name)
                # ✅ ใช้ get_all_values แทน get_all_records เพื่อความชัวร์
                raw_values = worksheet.get_all_values()
                if len(raw_values) > 1:
                    # ทำความสะอาดหัวตาราง (ตัด space)
                    headers = [h.strip() for h in raw_values[0]]
                    df_day = pd.DataFrame(raw_values[1:], columns=headers)
                    df_day['DataDate'] = pd.to_datetime(current)
                    all_days_list.append(df_day)
            except: pass 
            current += timedelta(days=1)
        return all_days_list, None
    except Exception as e: return None, str(e)

# ---------------------------------------------------------
# ส่วนที่ 1: เลือกชื่อร้านค้า (Global Selection)
# ---------------------------------------------------------
st.write("## 📋 ระบบจัดการข้อมูลร้านค้า")
selected_store = st.selectbox("📂 เลือกร้านค้า:", options=["ADN", "APB", "JYX", "JDX"])

yesterday = datetime.now().date() - timedelta(days=1)
today = datetime.now().date()

# ประกาศตัวแปร Global
combined_cust_data = None

tab1, tab2 = st.tabs(["ตรวจสอบยอดเทิร์น", "ข้อมูลลูกค้า"])

# ---------------------------------------------------------
# แท็บที่ 1: ตรวจสอบยอดเทิร์น
# ---------------------------------------------------------
with tab1:
    st.write("### 🔍 ตรวจสอบยอดเทิร์น")
    
    with st.form("streak_form_original"):
        col1, col2 = st.columns(2)
        with col1:
            target_val_input = st.text_input("ยอดเทิร์นขั้นต่ำ:", value="1,000", key="t1_val")
            s_date_s = st.date_input("📅 จากวันที่:", value=yesterday, key="t1_start")
        with col2:
            target_days_input = st.text_input("จำนวนวันที่ต้องต่อเนื่อง:", value="2", key="t1_days")
            e_date_s = st.date_input("📅 ถึงวันที่:", value=today, key="t1_end")
        submit_streak = st.form_submit_button("ค้นหา", use_container_width=True)

    # ✅ ย้าย block นี้เข้ามาอยู่ข้างใน tab1 ให้ถูกต้อง (ย่อหน้าให้ตรงกัน)
    if submit_streak:
        with st.spinner('กำลังประมวลผล...'):
            raw_list, err = fetch_raw_data_range(selected_store, s_date_s, e_date_s)
            
            if err: 
                st.error(err)
            elif not raw_list: 
                st.warning("ไม่พบข้อมูลในช่วงวันที่เลือก")
            else:
                df = pd.concat(raw_list, ignore_index=True)
                
                if 'Account' in df.columns and 'Valid Amount' in df.columns:
                    try:
                        target_val = float(target_val_input.replace(',', ''))
                        req_streak = int(target_days_input)
                        
                        df['Valid Amount'] = pd.to_numeric(df['Valid Amount'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                        
                        # 1. คำนวณ Streak
                        hit_df = df[df['Valid Amount'] >= target_val].sort_values(['Account', 'DataDate'])
                        hit_df['diff'] = hit_df.groupby('Account')['DataDate'].diff().dt.days
                        hit_df['is_new'] = hit_df['diff'] != 1
                        hit_df['grp'] = hit_df.groupby('Account')['is_new'].cumsum()
                        hit_df['streak_count'] = hit_df.groupby(['Account', 'grp']).cumcount() + 1
                        
                        hit_df['max_streak_in_grp'] = hit_df.groupby(['Account', 'grp'])['streak_count'].transform('max')
                        qualifying_days = hit_df[hit_df['max_streak_in_grp'] >= req_streak].copy()
                        
                        if not qualifying_days.empty:
                            # 3. ดึงเบอร์โทรศัพท์มา Match
                            cust_file_name = f"{selected_store} customer"
                            raw_cust_list, _ = fetch_raw_data_range(cust_file_name, s_date_s, e_date_s)
                            
                            phone_map = pd.DataFrame(columns=['match_key', 'เบอร์โทร'])
                            if raw_cust_list:
                                cust_df_all = pd.concat(raw_cust_list, ignore_index=True)
                                if 'username' in cust_df_all.columns and 'เบอร์โทร' in cust_df_all.columns:
                                    phone_map = cust_df_all[['username', 'เบอร์โทร']].copy()
                                    phone_map['match_key'] = phone_map['username'].astype(str).str.strip().str.lower()
                                    phone_map = phone_map.drop_duplicates('match_key')

                            qualifying_days['match_key'] = qualifying_days['Account'].astype(str).str.strip().str.lower()
                            final_merged = qualifying_days.merge(phone_map[['match_key', 'เบอร์โทร']], on='match_key', how='left')

                            final_merged['DailyText'] = (
                                final_merged['DataDate'].dt.strftime('%d') + 
                                ": " + 
                                final_merged['Valid Amount'].apply(lambda x: f"{x:,.0f}")
                            )

                            # 4. ยุบรวมรายการ
                            result_table = final_merged.groupby(['Account', 'เบอร์โทร', 'grp']).agg(
                                StartDate=('DataDate', 'min'),
                                EndDate=('DataDate', 'max'),
                                TotalValid=('Valid Amount', 'sum'),
                                DailyBreakdown=('DailyText', lambda x: " | ".join(x))
                            ).reset_index()

                            result_table['ช่วงวันที่'] = (
                                result_table['StartDate'].dt.strftime('%d') + 
                                " - " + 
                                result_table['EndDate'].dt.strftime('%d.%m.%Y')
                            )

                            final_display = result_table[['Account', 'เบอร์โทร', 'ช่วงวันที่', 'TotalValid', 'DailyBreakdown']]
                            final_display.columns = ['Account', 'เบอร์โทรศัพท์', 'ช่วงวันที่ตรวจพบ', 'ยอดเทิร์นรวม', 'รายละเอียดรายวัน (วันที่: ยอด)']

                            st.success(f"พบทั้งหมด {len(final_display)} รายการต่อเนื่อง")
                            
                            # --- 🚀 ส่วนที่แก้ไข: การแสดงผลตารางแบบซ่อนคอลัมน์เริ่มต้น ---
                            st.dataframe(
                                final_display.style.format({'ยอดเทิร์นรวม': '{:,.2f}'}),
                                hide_index=True, 
                                use_container_width=True,
                                # ✅ กำหนดลำดับคอลัมน์ที่จะให้ "แสดง" ตอนเริ่มต้น
                                # คอลัมน์ไหนที่ไม่ใส่ชื่อไว้ในนี้ (เช่น รายละเอียดรายวัน) จะถูกซ่อนไว้โดยอัตโนมัติ
                                column_order=['Account', 'เบอร์โทรศัพท์', 'ช่วงวันที่ตรวจพบ', 'ยอดเทิร์นรวม']
                            )

                        else:
                            st.info("💡 ไม่พบรายการที่มียอดต่อเนื่องถึงเกณฑ์ในช่วงวันที่เลือก")
                            
                    except Exception as e: 
                        st.error(f"เกิดข้อผิดพลาด: {e}")
                else:
                    st.error(f"ไฟล์ไม่มีคอลัมน์ Account/Valid Amount")

# ---------------------------------------------------------
# แท็บที่ 2: ข้อมูลลูกค้า (แก้ไขจุด Error 'res_display')
# ---------------------------------------------------------
with tab2:
    st.write("### 🔍 วิเคราะห์พฤติกรรมลูกค้า")
    
    # ประกาศตัวแปรสำหรับเก็บข้อมูลรวม
    combined_cust_data = None

    # --- ส่วนที่ 2.1: ค้นหาลูกค้าที่ยังไม่ฝาก (0 ติดต่อกัน) ---
    with st.expander("ค้นหาลูกค้าที่ยังไม่ฝากเงินต่อเนื่อง (ยอดฝากเป็น 0)", expanded=True):
        with st.form("no_deposit_search_form"):
            col_d1, col_d2, col_s = st.columns([1.5, 1.5, 1])
            with col_d1: s_date_no = st.date_input("📅 จากวันที่:", value=yesterday, key="t2_no_start")
            with col_d2: e_date_no = st.date_input("📅 ถึงวันที่:", value=today, key="t2_no_end")
            with col_s: streak_no_in = st.text_input("จำนวนวันต่อเนื่องที่ยอดฝากเป็น 0:", value="2", key="t2_no_streak")
            submit_no_dep = st.form_submit_button("ค้นหาลูกค้าไม่ฝาก", use_container_width=True)

        if submit_no_dep:
            raw_list, err = fetch_raw_data_range(f"{selected_store} customer", s_date_no, e_date_no)
            if err: st.error(err)
            elif raw_list:
                combined_cust_data = pd.concat(raw_list, ignore_index=True)
                df_work = combined_cust_data.copy()
                df_work['ฝากเงิน'] = pd.to_numeric(df_work['ฝากเงิน'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                
                no_dep_only = df_work[df_work['ฝากเงิน'] == 0].copy().sort_values(['username', 'DataDate'])
                no_dep_only['diff'] = no_dep_only.groupby('username')['DataDate'].diff().dt.days
                no_dep_only['grp'] = (no_dep_only['diff'] != 1).groupby(no_dep_only['username']).cumsum()
                no_dep_only['streak'] = no_dep_only.groupby(['username', 'grp']).cumcount() + 1
                
                target = int(streak_no_in)
                success = no_dep_only[no_dep_only['streak'] >= target]['username'].unique()
                
                if len(success) > 0:
                    st.success(f"พบลูกค้าที่ไม่ฝาก ต่อเนื่องกัน {target} วัน ทั้งหมด {len(success)} ราย")
                    # ✅ บรรทัดที่เพิ่มเข้ามา: สร้าง res_display เพื่อแก้ NameError
                    res_display = no_dep_only[no_dep_only['username'].isin(success)].groupby('username').last().reset_index()
                    st.dataframe(res_display[['username', 'ชื่อ', 'เบอร์โทร', 'streak']], 
                                 column_config={"streak": "ไม่ฝากต่อเนื่อง (วัน)"},
                                 hide_index=True, use_container_width=True)
                else: st.info("ไม่พบข้อมูลลูกค้าที่ไม่ฝากเงินตามเกณฑ์")

    # --- ส่วนที่ 2.2: ค้นหาลูกค้าที่ฝากต่อเนื่อง ---
    with st.expander("ค้นหาลูกค้าที่ฝากเงินต่อเนื่อง", expanded=True):
        with st.form("deposit_streak_form"):
            col_d1, col_d2, col_s = st.columns([1.5, 1.5, 1])
            with col_d1: s_date_dep = st.date_input("📅 จากวันที่:", value=yesterday, key="t2_dep_start")
            with col_d2: e_date_dep = st.date_input("📅 ถึงวันที่:", value=today, key="t2_dep_end")
            with col_s: streak_dep_in = st.text_input("จำนวนวันต่อเนื่องที่มีการฝากเงิน:", value="2", key="t2_dep_streak")
            submit_dep = st.form_submit_button("ค้นหาลูกค้าฝากต่อเนื่อง", use_container_width=True)

        if submit_dep:
            raw_list, err = fetch_raw_data_range(f"{selected_store} customer", s_date_dep, e_date_dep)
            if err: st.error(err)
            elif raw_list:
                combined_cust_data = pd.concat(raw_list, ignore_index=True)
                df_work = combined_cust_data.copy()
                df_work['ฝากเงิน'] = pd.to_numeric(df_work['ฝากเงิน'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                
                dep_only = df_work[df_work['ฝากเงิน'] > 0].copy().sort_values(['username', 'DataDate'])
                dep_only['diff'] = dep_only.groupby('username')['DataDate'].diff().dt.days
                dep_only['grp'] = (dep_only['diff'] != 1).groupby(dep_only['username']).cumsum()
                dep_only['streak'] = dep_only.groupby(['username', 'grp']).cumcount() + 1
                
                target = int(streak_dep_in)
                success = dep_only[dep_only['streak'] >= target]['username'].unique()
                
                if len(success) > 0:
                    st.success(f"พบคนฝากเงินต่อเนื่อง {target} วัน ทั้งหมด {len(success)} ราย")
                    res_display = dep_only[dep_only['username'].isin(success)].groupby('username').last().reset_index()
                    st.dataframe(res_display[['username', 'ชื่อ', 'เบอร์โทร', 'streak']], 
                                 column_config={"streak": "ฝากต่อเนื่อง (วัน)"},
                                 hide_index=True, use_container_width=True)
                else: st.info("ไม่พบข้อมูลลูกค้าที่ฝากเงินต่อเนื่องตามเกณฑ์")

    st.divider()

    # =========================================================
    # ส่วนที่ 3: VIP และ กำไร/ขาดทุน (ปรับความสูงตารางให้เท่ากัน)
    # =========================================================
    st.write("### 🏆 อันดับลูกค้า")

    today_now = datetime.now().date()
    yesterday_fixed = today_now - timedelta(days=1)
    start_7days_fixed = yesterday_fixed - timedelta(days=6)

    col_rank_left, col_rank_right = st.columns(2)

    # --- ฝั่งซ้าย: 💎 อันดับ VIP (แสดง 7 วันล่าสุดทันที และอัปเดตเมื่อค้นหา) ---
    with col_rank_left:
        with st.expander("💎 อันดับ VIP (กำหนดช่วงเวลาได้)", expanded=True):
            with st.form("vip_search_form"):
                v_col1, v_col2 = st.columns(2)
                with v_col1:
                    vip_start = st.date_input("📅 จากวันที่:", value=start_7days_fixed, key="v_start")
                with v_col2:
                    vip_end = st.date_input("📅 ถึงวันที่:", value=yesterday_fixed, key="v_end")
                vip_submit = st.form_submit_button("ค้นหาอันดับ VIP", use_container_width=True)

            with st.spinner('กำลังโหลดอันดับ VIP...'):
                vip_list, err_v = fetch_raw_data_range(f"{selected_store} customer", vip_start, vip_end)
                
                if err_v:
                    st.warning(err_v)
                elif vip_list:
                    v_df = pd.concat(vip_list, ignore_index=True)
                    v_df['ฝากเงิน'] = pd.to_numeric(v_df['ฝากเงิน'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                    v_summary = v_df.groupby(['username', 'ชื่อ', 'เบอร์โทร'])['ฝากเงิน'].sum().reset_index()
                    v_top20 = v_summary.nlargest(20, 'ฝากเงิน')
                    
                    # ✅ เพิ่ม height=725 เพื่อล็อคความสูงตาราง
                    st.dataframe(
                        v_top20.style.format({'ฝากเงิน': '{:,.2f}'}),
                        use_container_width=True,
                        hide_index=True,
                        height=425 
                    )
                else:
                    st.info("ไม่พบข้อมูลในช่วงที่เลือก")

    # --- ฝั่งขวา: 📈 อันดับกำไร/ขาดทุน (ล็อค 7 วันล่าสุด) ---
    with col_rank_right:
        with st.expander(f"📈 อันดับกำไร/ขาดทุน (7 วันล่าสุด: วันที่ {start_7days_fixed.day}-{yesterday_fixed.day})", expanded=True):
            pl_mode = st.radio("เลือกโหมด:", ["กำไรสูงสุด", "ขาดทุนสูงสุด"], horizontal=True, key="pl_mode_fixed")
            
            pl_list, err_pl = fetch_raw_data_range(f"{selected_store} customer", start_7days_fixed, yesterday_fixed)
            
            if err_pl:
                st.warning(err_pl)
            elif pl_list:
                pl_df = pd.concat(pl_list, ignore_index=True)
                pl_df['กำไรขาดทุน'] = pd.to_numeric(pl_df['กำไรขาดทุน'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                pl_summary = pl_df.groupby(['username', 'ชื่อ', 'เบอร์โทร'])['กำไรขาดทุน'].sum().reset_index()
                
                if pl_mode == "กำไรสูงสุด":
                    pl_top20 = pl_summary.nlargest(20, 'กำไรขาดทุน')
                else:
                    pl_top20 = pl_summary.nsmallest(20, 'กำไรขาดทุน')
                
                # ✅ เพิ่ม height=725 ให้เท่ากับตารางฝั่งซ้าย
                st.dataframe(
                    pl_top20.style.format({'กำไรขาดทุน': '{:,.2f}'}),
                    use_container_width=True,
                    hide_index=True,
                    height=514
                )
            else:
                st.info("ไม่พบข้อมูล 7 วันล่าสุด")