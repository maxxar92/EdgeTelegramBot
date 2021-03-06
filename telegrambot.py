import time
from timeloop import Timeloop
from datetime import timedelta
from functools import wraps
import json
import logging
import scrape_new_hosts as host_scraper
import pandas
from prettytable import PrettyTable
import staking
import geo_stat
from registered_updates import user_updater
from conversational import get_quote
from d3_plots_gen import gen_all_plots_js

from telegram import Bot, Update, ParseMode, ChatAction
from telegram.ext import  Dispatcher, Updater, CommandHandler, MessageHandler, Filters

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

logger = logging.getLogger(__name__)


with open('config.json') as config_file:
    data = json.load(config_file)

AUTH_TOKEN = data["auth"]
chat_id = data["chat_id"]

tl = Timeloop()
bot = Bot(AUTH_TOKEN)
dispatcher = Dispatcher(bot, update_queue=None, use_context=True)


# Define a few command handlers. These usually take the two arguments bot and
# update. Error handlers also receive the raised TelegramError object in error.
def start(update, context):
    update.message.reply_text(
        """This is a bot that will notify when new nodes are added to the network.""")

def help(update, context):
    update.message.reply_text(
        """This is a bot that will notify when new nodes are added to the network.

Commands:
/stargate <stargate> - Show hosts connected to this stargate.
/added <days> - Show statistics added hosts in last <days>.
/hosts - Show per-country statistics of current hosts.
/staked - Get the percentage of staked tokens from the total supply""", parse_mode=ParseMode.MARKDOWN)
   
def sendMessage(text):
    bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.MARKDOWN)


def error(update, context):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, context.error)

def send_action(action):
    """Sends `action` while processing func command."""
    def decorator(func):
        @wraps(func)
        def command_func(update, context, *args, **kwargs):
            context.bot.send_chat_action(chat_id=update.effective_message.chat_id, action=action)
            return func(update, context,  *args, **kwargs)
        return command_func
    
    return decorator

@tl.job(interval=timedelta(seconds=60))
def poll_explorer():
    new_hosts = host_scraper.poll_new_hosts(logger)
    if new_hosts is not None:
        for index, row in new_hosts.iterrows():
            print(row)
            logger.info(newHostMessage(row))
            sendMessage(newHostMessage(row))

@tl.job(interval=timedelta(seconds=60*60*4))
def poll_cmc():
    logger.info("polling_cmc")
    staking.check_new_prices(logger)

@tl.job(interval=timedelta(seconds=60*60 + 10))
def gen_js_plots():
    logger.info("generating js plots")
    gen_all_plots_js(logger)


def newHostMessage(host):
    # columns ["device_id", "host_name", "stargate", "location", "arch", "status"]
    print("hostname: ", host["host_name"])
    if host.location != "-":
        return "Host *{host.host_name}* (arch: {host.arch}) has joined the network from {host.location} and is connected to stargate *{host.stargate}*.".format(host=host)
    elif host.arch != "-":
        return "Host *{host.host_name}* (arch: {host.arch}) has joined the network from an unknown location and is connected to stargate *{host.stargate}*.".format(host=host)
    else:
        return "Host *{host.host_name}* has joined the network from an unknown location and is connected to stargate *{host.stargate}*.".format(host=host)

def get_stargate_hosts(update, context):
    telegram_id = update.message.chat_id
    if telegram_id == chat_id:
        update.message.reply_text("Only available in PM")
        return

    if len(context.args) == 0:
         update.message.reply_text("Not so fast! You must supply a stargate code.")
         return

    query_stargate = context.args[0]
    query_stargate = query_stargate.strip()
    logger.info('Stargate host request: %s', update.message.text)
    hosts_df = host_scraper.read_hosts_from_db()

    connected_hosts = hosts_df.loc[hosts_df.stargate == query_stargate]

    if connected_hosts.shape[0] == 0:
        update.message.reply_text("Stargate {} doesn't exist, or has no connected hosts.".format(query_stargate))
    else:
        x = PrettyTable(border=False, left_padding_width=1,right_padding_width=1)
        x.field_names = ["Host Name", "Location"]
        for index, row in connected_hosts.iterrows():
            x.add_row([row.host_name, row.location])

        out="```Hosts connected to {}\n{}```".format(query_stargate, str(x))
        update.message.reply_text(out, parse_mode=ParseMode.MARKDOWN)

@send_action(ChatAction.TYPING)
def get_stargate_hosts_map(update, context):
    if len(context.args) == 0:
         update.message.reply_text("Not so fast! You must supply a stargate code.")
         return

    query_stargate = context.args[0]
    query_stargate = query_stargate.strip()
    logger.info('Stargate host request: %s', update.message.text)
    
    hosts_df = host_scraper.read_hosts_from_db()
    connected_hosts = hosts_df.loc[hosts_df.stargate == query_stargate]
    if connected_hosts.shape[0] == 0:
        update.message.reply_text("Stargate {} doesn't exist, or has no connected hosts.".format(query_stargate))
        return
    try:
        stat_img_filename = "stargate_hosts.png"
        geo_stat.plot_stargate_hosts(stat_img_filename, logger, query_stargate)
    except Exception as e:
        logger.exception(e)
        update.message.reply_text("An error occured: {}".format(e))
        return

    update.message.reply_photo(photo=open(stat_img_filename, 'rb'))
    
@send_action(ChatAction.TYPING)
def get_added_stats(update, context):
    timeframe = 14
    if len(context.args) != 0:
        try:
            timeframe = int(context.args[0].strip())
            if timeframe < 1:
                update.message.reply_text("Error. Timeframe must be >= 0")
                return
        except ValueError:
            update.message.reply_text("Error. The number of days supplied must be a integer number.")
            return
    logger.info('Added host stats request: {} '.format(update.message.text))
    try:
        stat_img_filename = "added_nodes.png"
        geo_stat.plot_geostat_update(stat_img_filename, timeframe)
    except Exception as e:
        logger.exception(e)
        update.message.reply_text("An error occured: {}".format(e))
        return

    update.message.reply_photo(photo=open(stat_img_filename, 'rb'))

@send_action(ChatAction.TYPING)
def get_host_stats(update, context):
    try:
        stat_img_filename = "country_stats.png"
        geo_stat.plot_country_stat(stat_img_filename)
    except Exception as e:
        logger.exception(e)
        update.message.reply_text("An error occured: {}".format(e))
        return

    update.message.reply_photo(photo=open(stat_img_filename, 'rb'))

@send_action(ChatAction.TYPING)
def get_city_stats(update, context):
    try:
        stat_img_filename = "city_stats.png"
        geo_stat.plot_city_ranking(stat_img_filename)
    except Exception as e:
        logger.exception(e)
        update.message.reply_text("An error occured: {}".format(e))
        return

    update.message.reply_photo(photo=open(stat_img_filename, 'rb'))
    
    

def register_for_update(update, context):
    telegram_id = update.message.chat_id

    if telegram_id == chat_id:
        update.message.reply_text("This function can only be called in a private message to me.")
        return

    if len(context.args) == 1:
        try:
            token_str = context.args[0].strip()
            if len(token_str) != 10:
                raise ValueError
            device_token = int(token_str)
            user_updater.register(telegram_id, device_token)
            update.message.reply_text("Device {} has been registered".format(device_token))
        except ValueError:
            update.message.reply_text("Error. Token supplied must be a 10 digit integer number.")
    else:
        update.message.reply_text("Error. Must supply unique token generated by script. E.g. /register 123456789")

def unregister_from_update(update, context):
    telegram_id = update.message.chat_id

    if telegram_id == chat_id:
        update.message.reply_text("This function can only be called in a private message to me.")
        return

    if len(context.args) == 1:
        try:
            token_str = context.args[0].strip()
            if len(token_str) != 10:
                raise ValueError
            device_token = int(token_str)
            user_updater.unregister(telegram_id, device_token)
            update.message.reply_text("Device {} has been unregistered".format(device_token))
        except ValueError:
            update.message.reply_text("Error. Token supplied must be a integer number.")
    else:
        update.message.reply_text("Error. Must supply unique token generated by script. E.g. /unregister 123456789")
    
@send_action(ChatAction.TYPING)
def get_staked(update, context):
    try:
        stat_img_filename = "staked.png"
        staking.plot_staked(stat_img_filename)
    except Exception as e:
        logger.exception(e)
        update.message.reply_text("An error occured: {}".format(e))
        return

    update.message.reply_photo(photo=open(stat_img_filename, 'rb'))

@send_action(ChatAction.TYPING)
def get_payouts(update, context):
    try:
        stat_img_filename = "payouts.png"
        staking.plot_payouts(stat_img_filename)
    except Exception as e:
        logger.exception(e)
        update.message.reply_text("An error occured: {}".format(e))
        return

    update.message.reply_photo(photo=open(stat_img_filename, 'rb'))

def add_payout(update, context):
    telegram_id = update.message.chat_id

    if telegram_id != 399032132:
        update.message.reply_text("Sorry bro, you don't have admin rights")
        return

    if len(context.args) == 1:
        try:
            payout = int(context.args[0].strip())
            if payout < 1:
                update.message.reply_text("Error. Payout must be >= 0")
                return
        except ValueError:
            update.message.reply_text("Error. The payout supplied must be a integer number.")
            return
        staking.add_payout(payout)
        stat_img_filename = "payouts.png"
        staking.plot_payouts(stat_img_filename)
        update.message.reply_photo(photo=open(stat_img_filename, 'rb'))
    else:
        update.message.reply_text("Not so fast! You must supply a payout.")

def send_chat_message(update, context):
    telegram_id = update.message.chat_id

    if telegram_id != 399032132:
        update.message.reply_text("Sorry bro, you don't have admin rights")

    sendMessage(" ".join(context.args))



def get_funny_quote(update, context):
    reply_msg = update.message.reply_to_message
    #if reply_msg.from_user.id == bot.get_me().id:
    #    quote = get_quote()
    #    update.message.reply_text(quote)


def main():
    """Run bot."""
    # Create the Updater and pass it your bot's token.
    # Make sure to set use_context=True to use the new context based callbacks
    # Post version 12 this will no longer be necessary
    updater = Updater(AUTH_TOKEN, use_context=True)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    # on different commands - answer in Telegram
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("help", help))
    dp.add_handler(CommandHandler("stargatelist", get_stargate_hosts))
    dp.add_handler(CommandHandler("stargate", get_stargate_hosts_map))
    dp.add_handler(CommandHandler("added", get_added_stats))
    dp.add_handler(CommandHandler("hosts", get_host_stats))
    dp.add_handler(CommandHandler("cities", get_city_stats))
    dp.add_handler(CommandHandler("register", register_for_update))
    dp.add_handler(CommandHandler("unregister", unregister_from_update))
    dp.add_handler(CommandHandler("staked", get_staked))
    dp.add_handler(CommandHandler("payouts", get_payouts))
    dp.add_handler(CommandHandler("addpayout", add_payout))
    dp.add_handler(CommandHandler("sendmessage", send_chat_message))


    dp.add_handler(MessageHandler(Filters.reply, get_funny_quote))

    # log all errors
    dp.add_error_handler(error)

    # Start the Bot
    updater.start_polling()

    tl.start(block=False)

    user_updater.start_api_server(bot)

    # Block until you press Ctrl-C or the process receives SIGINT, SIGTERM or
    # SIGABRT. This should be used most of the time, since start_polling() is
    # non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == '__main__':
    main()
