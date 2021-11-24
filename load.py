import json



def init():

	with open('env_prod.json','r') as fp:
		env=json.load(fp)
	return env
