import win32com.client as win32
import sys
import os

recipient=sys.argv[1]
subject=sys.argv[2]
text=sys.argv[3]

outlook = win32.Dispatch('outlook.application')
mail = outlook.CreateItem(0)
mail.To = recipient
#Msg.CC = "moreaddresses here"
#Msg.BCC = "address"
mail.Subject = subject
mail.body = text.replace("*space*"," ").replace("*EOL*","\n")
mail.send