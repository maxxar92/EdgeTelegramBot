# Edge Telegram Bot 🤖 

<img src="https://github.com/maxxar92/EdgeTelegramBot/blob/master/edgebot_img.jpeg" width="200">

This is a Bot for informing the Edge Telegram Community about activities in the [Edge network](https://edge.network/en/).
As the edge API for querying nodes isn't published yet, this bot scrapes the [explorer](https://explorer.edge.network)  for changes periodically.

#### Commands
- /stargate <3-letter stargate> - Displays all hosts connected to the queried stargate
- /added \<days\> - Show statistics added hosts in last \<days\>.
- /hosts - Show per-country statistics of current hosts
- /register <10 number registry id> - Registers a device for [check-edge](https://github.com/befranz/check-edge) failure reports, check the link for setup guidelines.

#### Notifications
When a new node first comes online, a notification will be sent to the registered channel (currently the trading channel)


## Testing/Developing the Bot
Telegram Bots generally only allow one instance to run. As the main instance should be running at all time, a testing bot should be created to develop on a local bot first, before upgrading the real bot.

#### Creating a telegram bot for testing
Open a chat with @BotFather in Telegram and follow [this tutorial](https://riptutorial.com/telegram-bot/example/25075/create-a-bot-with-the-botfather) to create a bot. 
Enter the AUTH token into the auth field in config.json.
If you want to test push notifications (e.g. node going online), you will also need to supply a chat_id. This can be obtained by opening a chat with @RawDataBot and copy the chat.id to the config (this will be a private chat between you and your testing bot).

#### Running the bot
To deploy locally, first install the requirements, e.g. via pip ``pip install -r requirements.txt``.
Then start the bot with ``python telegrambot.py``.
Alternatively, the bot can be built and deployed using Docker. 
The hosts.db is persisted locally in the container, so rebuilding the image will lose the contents of the host.db. The `upgrade_container.sh` provides a script to copy the hosts.db to the host and rebuild and run the image.
Unit tests are run with ``python tests.py``.

## Contributing :rocket:

Contributions are very welcome! Please open a PR with a description what has been done, and if contributing a larger feature also add some unit tests in tests.py. Thank you!
For any further assistance, pm me @maxxar_92 in telegram.
