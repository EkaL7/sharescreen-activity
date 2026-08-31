@echo off
cd /d %~dp0
node server.js >> service.log 2>&1
