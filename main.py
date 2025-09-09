import os
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, ContextTypes, filters

# --- Secrets ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is not set. Use: fly secrets set BOT_TOKEN=...")

# --- Import AP Bot XP function ---
# Make sure AP Bot's add_xp function is accessible from this script
# For example, if AP Bot is a module or package installed:
from ap_bot_250826 import add_xp  # adjust import as needed

# --- Handler for messages ---
async def filter_mywin_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return

    caption = (message.caption or "").strip()
    has_image = message.photo or (message.document and message.document.mime_type.startswith("image"))

    # Only allow messages with an image AND caption starting with #mywin
    if has_image and caption.lower().startswith("#mywin"):
        parts = caption.split("#mywin", 1)
        game_name = parts[1].strip() if len(parts) > 1 else ""
        user_id = getattr(message.from_user, "id", None)

        if game_name and user_id:
            # Add XP directly to AP Bot
            add_xp(user_id, 20, game_name)
            await message.reply_text(f"✅ +20 XP for {game_name}!")
            return

    # Delete any other submission (text only, invalid caption, or missing image)
    await message.delete()

# --- Run bot ---
def main():
    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()
    app_bot.add_handler(MessageHandler(filters.ALL, filter_mywin_media))
    app_bot.run_polling(poll_interval=5)

if __name__ == "__main__":
    main()
