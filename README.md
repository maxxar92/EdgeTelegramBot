# Edge Telegram Bot 🤖 

<img src="https://github.com/maxxar92/EdgeTelegramBot/blob/master/edgebot_img.jpeg" width="200">

This is a Bot for informing the Edge Telegram Community about activities in the [Edge network](https://edge.network/en/).
As the edge API for querying nodes isn't published yet, this bot scrapes the [explorer](https://explorer.edge.network)  for changes periodically.

#### Commands
/stargate <3-letter stargate>  Displays all hosts connected to the queried stargate

#### Notifications
When a new node first comes online, a notification will be sent to the registered channel (currently the trading channel)


## Testing/Developing the Bot
Telegram Bots generally only allow one instance to run. As the main instance should be running at all time, a testing bot should be created to develop on a local bot first, before upgrading the real bot.

#### Creating a telegram bot for testing
Open a chat with @BotFather in Telegram and follow [this tutorial](https://riptutorial.com/telegram-bot/example/25075/create-a-bot-with-the-botfather) to create a bot. 
Enter the AUTH token into the auth field in config.json.
If you want to test push notifications (e.g. node going online), you will also need to supply a chat_id. This can be obtained by opening a chat with @RawDataBot and copy the chat.id to the config (this will be a private chat between you and your testing bot).

## Contributing :rocket:

Contributions are very welcome! Please open a PR with a description what has been done, and if contributing a larger feature also add some unit tests in tests.py. Thank you!
For any further assistance, pm me @maxxar_92 in telegram.
