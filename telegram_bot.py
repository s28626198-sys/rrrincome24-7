import os
import threading
import time
from datetime import datetime
import requests
import json
import re
import hashlib
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
import logging
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Try to import cloudscraper for Cloudflare bypass
try:
    import cloudscraper
    HAS_CLOUDSCRAPER = True
except ImportError:
    HAS_CLOUDSCRAPER = False

# Try to import curl_cffi for Cloudflare bypass (better than cloudscraper)
try:
    from curl_cffi import requests as curl_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Try to import cloudscraper for Cloudflare bypass
try:
    import cloudscraper
    HAS_CLOUDSCRAPER = True
except ImportError:
    HAS_CLOUDSCRAPER = False


# Supabase Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://sgnnqvfoajqsfdyulolm.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNnbm5xdmZvYWpxc2ZkeXVsb2xtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjQxNzE1MjcsImV4cCI6MjA3OTc0NzUyN30.dFniV0odaT-7bjs5iQVFQ-N23oqTGMAgQKjswhaHSP4")

user_jobs = {}  # Store monitoring jobs per user

# Global API client - single session for all users
global_api_client = None
api_lock = threading.Lock()

def get_global_api_client():
    """Get or create global API client (single session for all users)"""
    global global_api_client
    if global_api_client is None:
        global_api_client = APIClient()
        if not global_api_client.login():
            logger.error("Failed to login to API")
    return global_api_client

def refresh_global_token():
    """Refresh global API token if expired"""
    global global_api_client
    with api_lock:
        if global_api_client:
            if not global_api_client.login():
                logger.error("Failed to refresh API token")
                # Try to create new client
                global_api_client = APIClient()
                global_api_client.login()
        else:
            get_global_api_client()

def get_user_status(user_id):
    """Get user approval status from database"""
    try:

def add_user(user_id, username):
    """Add new user to database"""
    try:

def approve_user(user_id):
    """Approve user in database"""
    try:

def reject_user(user_id):
    """Reject user in database"""
    try:

def remove_user(user_id):
    """Remove user from database"""
    try:

def get_pending_users():
    """Get list of pending users"""
    try:
        return []

def get_all_users():
    """Get all users"""
    try:
        return []

def update_user_session(user_id, service=None, country=None, range_id=None, number=None, monitoring=0):
    """Update user session in database"""
    try:

def get_user_session(user_id):
    """Get user session from database"""
    try:
        return None

# API Functions (from otp_tool.py)
class APIClient:
    def __init__(self):
        self.base_url = BASE_URL
        # Use curl_cffi if available (best for Cloudflare bypass)
        if HAS_CURL_CFFI:
            self.session = curl_requests.Session(impersonate="chrome110")
            self.use_curl = True
            logger.info("Using curl_cffi for Cloudflare bypass")
        elif HAS_CLOUDSCRAPER:
            self.session = cloudscraper.create_scraper()
            self.use_curl = False
            logger.info("Using cloudscraper for Cloudflare bypass")
        else:
            self.session = requests.Session()
            self.use_curl = False
            logger.warning("No Cloudflare bypass available, using standard requests")
        self.auth_token = None
        self.email = API_EMAIL
        self.password = API_PASSWORD
        # Browser-like headers to avoid session expiration and Cloudflare - EXACT same as otp_tool.py
        self.browser_headers = {
            "User-Agent": "Mozilla/5.0 (Linux; Android 4.4.2; Nexus 4 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.114 Mobile Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-GB,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Origin": self.base_url,
            "Referer": f"{self.base_url}/dashboard/getnum",
            "X-Requested-With": "XMLHttpRequest",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty"
        }
    
    def login(self):
        """Login to API - EXACT COPY from otp_tool.py"""
        try:
            login_headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": self.browser_headers["User-Agent"],
                "Accept": self.browser_headers["Accept"],
                "Origin": self.browser_headers["Origin"],
                "Referer": f"{self.base_url}/auth/login"
            }
            login_resp = self.session.post(
                f"{self.base_url}/api/v1/mnitnetworkcom/auth/login",
                data={"email": self.email, "password": self.password},
                headers=login_headers,
                timeout=15
            )
            
            if login_resp.status_code in [200, 201]:
                login_data = login_resp.json()
                    self.auth_token = hitauth_data['data']['token']
                    
                    # Set account type cookie
                    self.session.cookies.set('mnitnetworkcom_accountType', 'user', domain='v2.mnitnetwork.com')
                    
                    # Store mhitauth token in cookie (browser does this)
                    self.session.cookies.set('mnitnetworkcom_mhitauth', self.auth_token, domain='v2.mnitnetwork.com')
                    
                    logger.info("Login successful")
                    return True
            return False
        except Exception as e:
            logger.error(f"Login error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def get_ranges(self, app_id):
        """Get active ranges for an application"""
        try:
            if not self.auth_token:
                if not self.login():
                    return []
            
            headers = {
                "mhitauth": self.auth_token,
                **{k: v for k, v in self.browser_headers.items() if k not in ["Origin", "Referer", "Content-Type"]}
            }
            headers["Origin"] = self.base_url
            headers["Referer"] = f"{self.base_url}/dashboard/getnum"
            
            resp = self.session.get(
                f"{self.base_url}/api/v1/mnitnetworkcom/dashboard/getac?type=carriers&appId={app_id}",
                headers=headers,
                timeout=15
            )
            
            # Check if token expired
            if resp.status_code == 401 or (resp.status_code == 200 and 'expired' in resp.text.lower()):
                logger.info("Token expired, refreshing...")
                if self.login():
                    # Retry request
                    resp = self.session.get(
                        f"{self.base_url}/api/v1/mnitnetworkcom/dashboard/getac?type=carriers&appId={app_id}",
                        headers=headers,
                        timeout=15
                    )
            
            if resp.status_code == 200:
                data = resp.json()
                if 'data' in data and data['data'] is not None:
                    return data['data']
            return []
        except Exception as e:
            logger.error(f"Error getting ranges: {e}")
            return []
    
    def get_number(self, range_id):
        """Request a number from a range"""
        try:
            if not self.auth_token:
                if not self.login():
                    return None
            
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "mhitauth": self.auth_token,
                **{k: v for k, v in self.browser_headers.items() if k != "Content-Type"}
            }
            headers["Referer"] = f"{self.base_url}/dashboard/getnum?range={range_id}"
            
            resp = self.session.post(
                f"{self.base_url}/api/v1/mnitnetworkcom/dashboard/getnum",
                data={
                    "range": range_id,
                    "national": "false",
                    "removePlus": "false",
                    "app": "null",
                    "carrier": "null"
                },
                headers=headers,
                timeout=15
            )
            
            if resp.status_code == 200:
                data = resp.json()
                if 'data' in data:
                    number_data = data['data']
                    if isinstance(number_data, dict):
                        if 'number' in number_data:
                            return number_data
                        elif 'num' in number_data and isinstance(number_data['num'], list) and len(number_data['num']) > 0:
                            return number_data['num'][0]
                    elif isinstance(number_data, list) and len(number_data) > 0:
                        return number_data[0]
            return None
        except Exception as e:
            logger.error(f"Error getting number: {e}")
            return None
    
        try:
            if not self.auth_token:
                if not self.login():
                    return None
            
            today = datetime.now().strftime("%d_%m_%Y")
            timestamp = int(time.time() * 1000)
            
            headers = {
                **{k: v for k, v in self.browser_headers.items() if k not in ["Origin", "Referer", "Content-Type"]}
            }
            headers["Origin"] = self.base_url
            headers["Referer"] = f"{self.base_url}/dashboard/getnum"
            
            
            if resp.status_code == 200:
                data = resp.json()
                if 'data' in data and data['data'] is not None:
                    data_obj = data['data']
                    if isinstance(data_obj, dict) and 'num' in data_obj and data_obj['num'] is not None:
                        numbers = data_obj['num']
                        if isinstance(numbers, list):
                            target_normalized = number.replace('+', '').replace(' ', '').replace('-', '').strip()
                                        num_digits = ''.join(filter(str.isdigit, num_value))
                                        if len(num_digits) >= 9 and num_digits[-9:] == target_digits[-9:]:
                                            return num_data
            return None
        except Exception as e:
            logger.error(f"Error checking OTP: {e}")
            return None

async def admin_commands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle admin commands"""
    user_id = update.effective_user.id
    
    if user_id != ADMIN_USER_ID:
        await update.message.reply_text("❌ Access denied. Admin only.")
        return
    
    command = update.message.text.split()[0] if update.message.text else ""
    
    if command == "/users":
        users = get_all_users()
        if not users:
            await update.message.reply_text("📋 No users found.")
            return
        
        message = "📋 All Users:\n\n"
        for uid, uname, status in users:
            message += f"ID: {uid}\n"
            message += f"Username: @{uname or 'N/A'}\n"
            message += f"Status: {status}\n"
            message += f"{'─' * 20}\n"
        
        await update.message.reply_text(message[:4000])  # Telegram limit
    
    elif command.startswith("/remove"):
        try:
            target_id = int(context.args[0]) if context.args else None
            if target_id:
                # Stop any monitoring jobs for this user
                if target_id in user_jobs:
                    user_jobs[target_id].schedule_removal()
                    del user_jobs[target_id]
                remove_user(target_id)
                await update.message.reply_text(f"✅ User {target_id} removed successfully.")
            else:
                await update.message.reply_text("Usage: /remove <user_id>")
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")
    
    elif command == "/pending":
        pending = get_pending_users()
        if not pending:
            await update.message.reply_text("✅ No pending users.")
            return
        
        message = "⏳ Pending Users:\n\n"
        for uid, uname in pending:
            message += f"ID: {uid} - @{uname or 'N/A'}\n"
        
        await update.message.reply_text(message)

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button callbacks"""
    query = update.callback_query
    
    data = query.data
    user_id = query.from_user.id
    
    # Admin actions
    if data.startswith("admin_"):
        if user_id != ADMIN_USER_ID:
            await query.edit_message_text("❌ Access denied.")
            return
        
        if data.startswith("admin_approve_"):
            target_user_id = int(data.split("_")[2])
            approve_user(target_user_id)
            await query.edit_message_text(f"✅ User {target_user_id} approved.")
            try:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text="✅ Your request has been approved! Use /start to begin."
                )
            except:
                pass
        
        elif data.startswith("admin_reject_"):
            target_user_id = int(data.split("_")[2])
            reject_user(target_user_id)
            await query.edit_message_text(f"❌ User {target_user_id} rejected.")
            try:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text="❌ Your request has been rejected."
                )
            except:
                pass
        return
    
    # Check if user is approved
    status = get_user_status(user_id)
    if status != 'approved':
        await query.edit_message_text("❌ Your access is pending approval.")
        return
    
    # Service selection
    if data.startswith("service_"):
        service_name = data.split("_")[1]
        
        # Group ranges by country - detect from range name if country not available
        country_ranges = {}
        for r in ranges:
            range_name = r.get('name', r.get('id', ''))
            # Try to get country from API response first
            country = r.get('cantryName', r.get('country', ''))
            
            # If country not found or Unknown, detect from range name
            if not country or country == 'Unknown' or country.strip() == '':
                country = detect_country_from_range(range_name)
            
            # Only use Unknown as last resort - try harder to detect
            if not country or country == 'Unknown':
                # Try to extract from range name more aggressively
                range_str = str(range_name).upper()
                # Sometimes range name contains country code in different format
                for code, country_name in COUNTRY_CODES.items():
                    if code in range_str or country_name.upper() in range_str:
                        country = country_name
                        break
            
            # Final fallback - use detected or keep as Unknown
            if not country:
                country = 'Unknown'
            
            if country not in country_ranges:
                country_ranges[country] = []
            country_ranges[country].append(r)
        
        # Create country buttons - INLINE KEYBOARD
        keyboard = []
        # Filter out Unknown countries - try to detect them first
        country_list = []
        for country in sorted(country_ranges.keys()):
            if country != 'Unknown':
                country_list.append(country)
        
        # Only add Unknown if we really can't detect any country
        if 'Unknown' in country_ranges and len(country_list) == 0:
            country_list.append('Unknown')
        
        # Create inline keyboard rows (2 buttons per row)
        for i in range(0, len(country_list), 2):
            row = []
            flag1 = get_country_flag(country_list[i])
            row.append(InlineKeyboardButton(f"{flag1} {country_list[i]}", callback_data=f"country_{service_name}_{country_list[i]}"))
            if i + 1 < len(country_list):
                flag2 = get_country_flag(country_list[i + 1])
                row.append(InlineKeyboardButton(f"{flag2} {country_list[i + 1]}", callback_data=f"country_{service_name}_{country_list[i + 1]}"))
            keyboard.append(row)
        
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_services")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            f"📱 {service_name.upper()} - Select Country:",
            reply_markup=reply_markup
        )
    
        parts = data.split("_", 2)
        service_name = parts[1]
        country = parts[2]
        
        # Stop any existing monitoring jobs for this user
        if user_id in user_jobs:
            old_job = user_jobs[user_id]
            old_job.schedule_removal()
            del user_jobs[user_id]
        
        service_map = {
            "whatsapp": "verifyed-access-whatsapp",
            "facebook": "verifyed-access-facebook",
            "telegram": "verifyed-access-telegram"
        }
        
        app_id = service_map.get(service_name)
        if not app_id:
            await query.edit_message_text("❌ Invalid service.")
            return
        
        # Get global API client
        api_client = get_global_api_client()
        if not api_client:
            await query.edit_message_text("❌ API connection error. Please try again.")
            return
        
        with api_lock:
            ranges = api_client.get_ranges(app_id)
        
        
        if not selected_range:
            await query.edit_message_text(f"❌ No ranges found for {country}.")
            return
        
        range_id = selected_range.get('name', selected_range.get('id', ''))
        )
    
    # Back to services
    elif data == "back_services":
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "✅ Please select a service:",
            reply_markup=reply_markup
        )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text messages (keyboard button presses)"""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    
    # Check if user is approved
    status = get_user_status(user_id)
    if status != 'approved':
        await update.message.reply_text("❌ Your access is pending approval.")
        return
    
    if text in ["💬 WhatsApp", "👥 Facebook", "✈️ Telegram"]:
        service_map = {
            "💬 WhatsApp": "whatsapp",
            "👥 Facebook": "facebook",
            "✈️ Telegram": "telegram"
        }
        service_name = service_map[text]
        app_id_map = {
            "whatsapp": "verifyed-access-whatsapp",
            "facebook": "verifyed-access-facebook",
            "telegram": "verifyed-access-telegram"
        }
        app_id = app_id_map.get(service_name)
        
        # Get global API client
        api_client = get_global_api_client()
        if not api_client:
            await update.message.reply_text("❌ API connection error. Please try again.")
            return
        
        try:
            with api_lock:
                ranges = api_client.get_ranges(app_id)
            
            if not ranges:
                await update.message.reply_text(f"❌ No active ranges available for {service_name}.")
                return
            
            # Group ranges by country - detect from range name
            country_ranges = {}
            for r in ranges:
                range_name = r.get('name', r.get('id', ''))
                country = r.get('cantryName', r.get('country', ''))
                
                # If country not found or Unknown, detect from range name
                if not country or country == 'Unknown' or country.strip() == '':
                    country = detect_country_from_range(range_name)
                
                # Only use Unknown as last resort - try harder to detect
                if not country or country == 'Unknown':
                    # Try to extract from range name more aggressively
                    range_str = str(range_name).upper()
                    # Sometimes range name contains country code in different format
                    for code, country_name in COUNTRY_CODES.items():
                        if code in range_str or country_name.upper() in range_str:
                            country = country_name
                            break
                
                # Final fallback - use detected or keep as Unknown
                if not country:
                    country = 'Unknown'
                
                if country not in country_ranges:
                    country_ranges[country] = []
                country_ranges[country].append(r)
            
            # Create country buttons - INLINE KEYBOARD
            keyboard = []
            # Filter out Unknown countries - try to detect them first
            country_list = []
            for country in sorted(country_ranges.keys()):
                if country != 'Unknown':
                    country_list.append(country)
            
            # Only add Unknown if we really can't detect any country
            if 'Unknown' in country_ranges and len(country_list) == 0:
                country_list.append('Unknown')
            
            # Create inline keyboard rows (2 buttons per row)
            for i in range(0, len(country_list), 2):
                row = []
                flag1 = get_country_flag(country_list[i])
                row.append(InlineKeyboardButton(f"{flag1} {country_list[i]}", callback_data=f"country_{service_name}_{country_list[i]}"))
                if i + 1 < len(country_list):
                    flag2 = get_country_flag(country_list[i + 1])
                    row.append(InlineKeyboardButton(f"{flag2} {country_list[i + 1]}", callback_data=f"country_{service_name}_{country_list[i + 1]}"))
                keyboard.append(row)
            
            keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_services")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                f"📱 {service_name.upper()} - Select Country:",
                reply_markup=reply_markup
            )
        except Exception as e:
            logger.error(f"Error in handle_message service selection: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")
    
                reply_markup=reply_markup
            )
            return
        
        # Extract country name from button text (remove flag)
        country = re.sub(r'^[🇦-🇿\s]+', '', text).strip()
        
        # Get service from user session
        session = get_user_session(user_id)
        service_name = session.get('service') if session else None
        
        if not service_name:
            # Try to detect - for now default to whatsapp
            service_name = "whatsapp"
        
        service_map = {
            "whatsapp": "verifyed-access-whatsapp",
            "facebook": "verifyed-access-facebook",
            "telegram": "verifyed-access-telegram"
        }
        app_id = service_map.get(service_name)
        
        # Stop any existing monitoring jobs
        if user_id in user_jobs:
            old_job = user_jobs[user_id]
            old_job.schedule_removal()
            del user_jobs[user_id]
        
        # Get global API client
        api_client = get_global_api_client()
        if not api_client:
            await update.message.reply_text("❌ API connection error. Please try again.")
            return
        
        try:
            with api_lock:
                ranges = api_client.get_ranges(app_id)
            
            
            if not selected_range:
                await update.message.reply_text(f"❌ No ranges found for {country}.")
                return
            
            range_id = selected_range.get('name', selected_range.get('id', ''))
                monitor_otp,
                interval=2,
                first=2,
                chat_id=user_id,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Error in handle_message country selection: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

async def monitor_otp(context: ContextTypes.DEFAULT_TYPE):
        job.schedule_removal()
        if user_id in user_jobs:
            del user_jobs[user_id]
        update_user_session(user_id, monitoring=0)
        try:
            )
        except:
            pass
        return
    
    # Get global API client
    api_client = get_global_api_client()
    if not api_client:
        return
    
    try:
    except Exception as e:
        logger.error(f"Error monitoring OTP for user {user_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())

    
    # Create application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))

if __name__ == "__main__":
    main()

