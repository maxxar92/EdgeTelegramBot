import random

def get_quote():
	with open("quotes.txt", "r") as quotes:
		lines = quotes.readlines()
		return random.choice(lines).strip()

if __name__ == "__main__":
	print(get_quote())