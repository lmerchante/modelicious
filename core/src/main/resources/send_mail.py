import win32com.client as win32
import sys
import os

import xml.dom.minidom
import HTMLParser

recipient=sys.argv[1]
subject=sys.argv[2]
xml_file=sys.argv[3]
html_file=sys.argv[4]

# Pretty print the file - Open and overwrite
text = ""
with open(xml_file, 'r+') as f:
    text = f.read()
    text = xml.dom.minidom.parseString(text).toprettyxml()
    text = HTMLParser.HTMLParser().unescape(text)
    f.seek(0)
    f.write(text)
    f.truncate()

with open(html_file, 'r+') as f:
    text = f.read()


outlook = win32.Dispatch('outlook.application')
mail = outlook.CreateItem(0)
mail.To = recipient
#Msg.CC = "moreaddresses here"
#Msg.BCC = "address"
mail.Subject = subject
#mail.HTMLBody = text
mail.Body = text 
mail.Attachments.Add( html_file )
mail.send
