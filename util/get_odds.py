def get_odds_win(soup):
    odds = {}
    rows = soup.find_all('table', {'class': 'is-w495'})[0].find_all('tbody')
    for row in rows:
        cell = row.find_all('td')
        if cell[2] != '欠場':
            odds[cell[0].get_text()] = float(cell[2].get_text())
    return odds

def get_odds_place(soup):
    odds = {}
    rows = soup.find_all('table', {'class': 'is-w495'})[1].find_all('tbody')
    for row in rows:
        cell = row.find_all('td')
        odds[cell[0].get_text()] = [float(cell[2].get_text().split('-')[0]), float(cell[2].get_text().split('-')[1])]
    return odds

def get_odds_wide(soup):
    odds = {}
    
    ar = [s.get_text() for s in soup.find_all('tbody', {'class': 'is-p3-0'})[0].find_all('td',{'class':'oddsPoint'})]
    cmb = []
    a = 1
    b = 2
    for i in range(5):
        for n in range(i+1):
            cmb.append(str(a+n)+'-'+str(b))
        b += 1
    
    for a, c in zip(ar, cmb):
        odds[c] = [float(a.split('-')[0]), float(a.split('-')[1])]
    return odds
    
def get_odds_triple(soup):
    odds = {}
    ar = []
    cell = soup.find_all('td',{'class':'oddsPoint'})

    for i in range(int(len(cell)/6)):
        ar.append([float(c.get_text()) for c in cell[i*6:i*6+6]])
    ar = np.ndarray.flatten(np.array(ar).T).tolist()
    
    cmb = []
    for a in range(6):
        for b in range(6):
            for c in range(6):
                if (a != b) and (b != c) and (a != c):
                    cmb.append(str(a+1) +'-'+ str(b+1) +'-'+ str(c+1))
     
    for a, c in zip(ar, cmb):
        odds[c] = a
    return odds