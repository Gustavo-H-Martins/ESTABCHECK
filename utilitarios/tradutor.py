from httpx import HTTPError
import translators.server as tss
def traduzir(texto:str, idioma_origem:str= 'en', idioma_destino:str = 'pt'):
    """ Traduz um texto informado de um idioma para outro
    idiomas suportados:
    Language	Language of Translator
    english	en
    chinese	zh
    arabic	ar
    russian	ru
    french	fr
    german	de
    spanish	es
    portuguese	pt
    italian	it
    japanese	ja
    korean	ko
    greek	el
    dutch	nl
    hindi	hi
    turkish	tr
    malay	ms
    thai	th
    vietnamese	vi
    indonesian	id
    hebrew	he
    polish	pl
    mongolian	mn
    czech	cs
    hungarian	hu
    estonian	et
    bulgarian	bg
    danish	da
    finnish	fi
    romanian	ro
    swedish	sv
    slovenian	sl
    persian/farsi	fa
    bosnian	bs
    serbian	sr
    fijian	fj
    filipino	tl
    haitiancreole	ht
    catalan	ca
    croatian	hr
    latvian	lv
    lithuanian	lt
    urdu	ur
    ukrainian	uk
    welsh	cy
    tahiti	ty
    tongan	to
    swahili	sw
    samoan	sm
    slovak	sk
    afrikaans	af
    norwegian	no
    bengali	bn
    malagasy	mg
    maltese	mt
    queretaro otomi	otq
    klingon/tlhingan hol	tlh
    gujarati	gu
    tamil	ta
    telugu	te
    punjabi	pa
    amharic	am
    azerbaijani	az
    bashkir	ba
    belarusian	be
    cebuano	ceb
    chuvash	cv
    esperanto	eo
    basque	eu
    irish	ga
    emoji	emj

    """
    if not isinstance(texto, str):
        print("Erro: o parâmetro 'texto' deve ser uma string.")
        return texto

    if not texto:
        return texto

    try:
        traducao = tss.google(texto, from_language=f'{idioma_origem}', to_language=f'{idioma_destino}')
    except HTTPError as e:
        print(f"Erro de conexão com a API de tradução: {e}")
        return texto

    if isinstance(traducao, dict) and 'data' in traducao and len(traducao['data']) > 0:
        return traducao['data']
    else:
        print(f"Erro ao traduzir: {traducao}")
        return traducao
