import pandas as pd
import urllib.request
import json


gallery_doc_url = "https://docs.google.com/spreadsheets/d/1h7aTm0p88Hkh-nA9kAKKdTHpdJ3rTH0ttIvuaaXZxyo/export?format=csv&id=1h7aTm0p88Hkh-nA9kAKKdTHpdJ3rTH0ttIvuaaXZxyo&gid=0"
gallery = pd.read_csv(gallery_doc_url)

url_list = gallery["Link"].values

gallery_json = {"data": []}
for v in url_list:
    s = v.split("/")
    gallery_item = {}
    gallery_item["url_root"] = "%s//%s/%s/%s/" % (s[0], s[2], s[3], s[4])
    gallery_item["url_part"] = "%s/%s/%s" % (s[5], s[6], s[7])
    gallery_item["file_name"] = s[7].split(".")[0]
    gallery_json["data"].append(gallery_item)

with open("data/gallery.json", "w") as f:
    json.dump(gallery_json, f)
