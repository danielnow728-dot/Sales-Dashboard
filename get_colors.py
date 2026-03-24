import urllib.request
import re
from collections import Counter

url = "https://cdspecialtycontractors.com/"
req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
try:
    html = urllib.request.urlopen(req).read().decode('utf-8', errors='ignore')
    
    colors = re.findall(r'#[0-9A-Fa-f]{6}', html)
    colors = [c.upper() for c in colors]
    
    print("--- TOP 15 HEX COLORS IN HTML SOURCE ---")
    for c, count in Counter(colors).most_common(15):
        print(f"{c}: {count} occurrences")
        
    print("\n--- BRANDING CLUES ---")
    if 'primary' in html.lower(): print("Found 'primary' references.")
    if 'secondary' in html.lower(): print("Found 'secondary' references.")
    
except Exception as e:
    print("Error:", e)
