Set oWS = WScript.CreateObject("WScript.Shell")
desktopFolder = oWS.SpecialFolders("Desktop")
sLinkFile = desktopFolder & "\Sales Dashboard.lnk"
Set oLink = oWS.CreateShortcut(sLinkFile)
oLink.TargetPath = "C:\Users\danie\.gemini\antigravity\playground\Sale Meeting\Run Dashboard.bat"
oLink.WorkingDirectory = "C:\Users\danie\.gemini\antigravity\playground\Sale Meeting"
oLink.Description = "Launch the Sales Meeting Dashboard"
oLink.Save
