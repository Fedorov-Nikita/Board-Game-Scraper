import requests as re
from bs4 import BeautifulSoup


def clear_link(link: str) -> str:
    if link.find('://') >= 0:
        link = link[link.find('://') + 3:]
    if link[:4] == 'www.':
        link = link[4:]
    return link.strip("/")


def get_user_profile(nickname: str) -> dict:
    """
	Get dict with user profile data from boardgamegeek.com
	In format:
		{'registration_date': '1970-01-01',
		 'last_profile_update': '1970-01-01',
		 'last_login': '1970-01-01',
		 'country': 'England',
		 'state': 'smwh',
		 'city': 'smwh',
		 'website': 'https://www.site.me.com.smth',
		 'geekmail': 'Send Private Message to nickname'}
	"""

    url_main = 'https://boardgamegeek.com/user/'

    r = re.get(url_main + nickname)
    soup = BeautifulSoup(r.text, features='lxml')
    table = soup.find('table', class_='profile_table')
    rows = table.find_all('tr')
    user_dict = {}
    for row in rows:
        info = row.find_all('td')
        user_dict[info[0].text[:-1].lower().replace(' ', '_').split('/')[-1]] = info[1].text.strip()
    user_dict['website'] = clear_link(user_dict['website'])
    if user_dict['website'] != '':
        if re.get('https://' + user_dict['website']).status_code != 200:
            user_dict['website'] = ''
    return user_dict
