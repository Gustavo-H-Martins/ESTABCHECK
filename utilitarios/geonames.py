def get_geolocalizacao(idioma:str = 'pt', pais:str = 'br',cod_uf_geonames:str=15, cod_municipio:str=3106200, locais:str="P", usuario:str='georchestra'):
	import requests
	url = "https://secure.geonames.org/searchJSON?"
	params = {
		"lang":idioma,
		"country":pais,
		#"adminCode1":cod_uf_geonames,
		"adminCode2":cod_municipio,
		"style":"full",
		"maxRows":1000,
		"featureClass":locais,
		"type":"json",
		"username": usuario
		}
	headers = {
		'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
		'Accept-Encoding': 'gzip, deflate, br',
		'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
		'Cache-Control': 'max-age=0',
		'Connection': 'keep-alive',
		'Host': 'secure.geonames.org',
		'Sec-Fetch-Dest': 'document',
		'Sec-Fetch-Mode': 'navigate',
		'Sec-Fetch-Site': 'none',
		'Sec-Fetch-User': '?1',
		'Upgrade-Insecure-Requests': '1',
		'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
		'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
		'sec-ch-ua-mobile': '?0',
		'sec-ch-ua-platform': '"Windows"'
	}

	response = requests.request("GET", url, headers=headers, params=params)
	data = response.json()['geonames']
	base = []
	for d in data:
		base.append({
			"UF" : d["adminCodes1"]["ISO3166_2"],
			"COD_UF" : d["adminCode1"],
			"MUNICIPIO" : d["adminCode2"],
			"BAIRRO" : d["asciiName"].upper(),
			"LATITUDE": d["lat"],
			"LONGITUDE": d["lng"]
		})
	return base
