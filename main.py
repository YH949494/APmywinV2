import os
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, ContextTypes, filters

# --- Secrets ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is not set. Use: fly secrets set BOT_TOKEN=...")

# ----------------------------
# MyWin Media Handler
# ----------------------------
async def filter_mywin_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return

    caption = (message.caption or "").strip()
    has_image = (
        message.photo
        or (message.document and message.document.mime_type.startswith("image"))
    )

    # Only allow image + proper caption
    if has_image and caption.lower().startswith("#mywin"):
        parts = caption.split("#mywin", 1)
        game_name = parts[1].strip() if len(parts) > 1 else ""

        if game_name:
            # ✅ Directly update XP in your existing MongoDB users collection
            users_collection.update_one(
                {"user_id": message.from_user.id},
                {"$inc": {"xp": 20, "weekly_xp": 20, "monthly_xp": 20}},
                upsert=True,
            )

            await message.reply_text(f"✅ +20 XP for {game_name}!")
            return

    # Delete all invalid submissions
    await message.delete()

# --- Run bot ---
def main():
    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()
    app_bot.add_handler(MessageHandler(filters.ALL, filter_mywin_media))
    app_bot.run_polling(poll_interval=5)

if __name__ == "__main__":
    main()
