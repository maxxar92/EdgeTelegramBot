from etherscan.accounts import Account
import json
import matplotlib.pyplot as plt

def get_stakes():
	with open('config.json') as config_file:
	    data = json.load(config_file)

	if "etherscan_api_token" not in data.keys() or data['etherscan_api_token'] == "":
		print("etherscan_api_token must be specified in config.json")
		return 

	key = data['etherscan_api_token']
	stake_address = '0x370f4d7625c10499bbb9353f3e91f83a0b750bec'

	api = Account(address=stake_address, api_key=key)
	transactions = api.get_transaction_page(page=1, offset=10000, sort='des', erc20=True)
	total_in = 0
	total_out = 0
	stargates=0
	for t in transactions:
		if t["tokenName"] == "DADI" and t["to"] == stake_address:
			value = int(t["value"]) / 1e18
			total_in += value
			if value == 5e5:
				stargates += 1
		elif t["from"] == stake_address and t["to"] != "0xef45b79def79a2cd2d0c77f84fddaab8d0b8be35":
			total_out = value
			if value == 5e5:
				stargates -= 1
	total_staked = total_in - total_out 
	stargates_staked = stargates * 5e5
	hosts_staked = total_staked - stargates_staked

	return total_staked, hosts_staked, stargates_staked

def plot_staked(out_filename):
	total_supply = 100e6
	total_staked, host_stake, stargate_stake = get_stakes()

	# Pie chart, where the slices will be ordered and plotted counter-clockwise:
	labels = 'Non-Staked', 'Stargates', 'Hosts'
	sizes = [total_supply-total_staked, stargate_stake, host_stake]
	explode = (0, 0.1, 0.1) 

	fig, ax = plt.subplots()
	ax.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',
	        shadow=False, startangle=0)
	ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

	fig.savefig(out_filename,dpi=200,bbox_inches="tight")
	fig.close()