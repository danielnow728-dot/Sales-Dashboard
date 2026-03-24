import urllib.request
import re

url = "https://cdspecialtycontractors.com/"
req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
try:
    html = urllib.request.urlopen(req).read().decode('utf-8', errors='ignore')
    
    # Search for image tags containing 'logo' and ending in png/jpg/webp
    logos = re.findall(r'src=["\']([^"\']*?logo[^"\']*?\.(?:png|jpg|jpeg|webp))["\']', html, re.IGNORECASE)
    
    if logos:
        logo_url = logos[0]
        if logo_url.startswith('/'):
            logo_url = "https://cdspecialtycontractors.com" + logo_url
        print("Scraped logo URL:", logo_url)
        
        # Add basic headers for the image request
        img_req = urllib.request.Request(logo_url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(img_req) as response, open('logo.png', 'wb') as out_file:
            out_file.write(response.read())
            
        print("Successfully downloaded to logo.png")
    else:
        print("Could not automatically locate the logo URL.")
except Exception as e:
    print("Error:", e)
