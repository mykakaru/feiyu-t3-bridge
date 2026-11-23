"""Admission for one low-priority interactive seat, in addition to two regular seats."""
import ctypes
import os
import re
import time
from ctypes import wintypes

def class_for(text,manifest):
    if len(text)>800 or manifest.get('chat_delivery')!='bridge_reports_first_and_final':
        return ''
    if manifest.get('related') or re.search(r'批量|全量|所有|渲染|动画|动捕|视频|转码|编译|代码|程序|脚本|工程|运行命令|压缩包|扫描|部署|爬取|游戏|模型|Maya|Unreal|ffmpeg|PowerShell|Python|执行.{0,4}命令',text,re.I):
        return ''
    members=manifest.get('members',[])
    if any(m.get('quotes') for m in members):return ''
    attachments=[a for m in members for a in m.get('attachments',[])]
    if not attachments and re.search(r'(?:企微|在线).{0,8}(?:文档|表格)|(?:doc|page)\.weixin\.qq\.com/',text,re.I) and re.search(r'新建|创建|读回|读取|写入|追加|改名|查询|核对',text):
        return 'document'
    if (len(attachments)==1 and attachments[0].get('type')=='file' and attachments[0].get('state')=='downloaded'
            and 0 < (attachments[0].get('bytes') or 0) <= 8*1024*1024
            and re.search(r'转发|发回|发给',text) and re.search(r'给我|回我|本私聊|这个私聊|本人',text)):
        return 'return_file'
    if not attachments and len(text)<140 and re.search(r'\d{1,8}\s*[×÷+*/－-]\s*\d{1,8}',text) and re.search(r'计算|算出|算式|多少',text):
        return 'arithmetic'
    return ''

class Memory(ctypes.Structure):
    _fields_=[('length',wintypes.DWORD),('load',wintypes.DWORD)]+[(n,ctypes.c_ulonglong) for n in ('total','free','total_page','free_page','total_virtual','free_virtual','extended')]

def system_times(kernel):
    idle,kernel_time,user=(ctypes.c_ulonglong() for _ in range(3))
    if not kernel.GetSystemTimes(ctypes.byref(idle),ctypes.byref(kernel_time),ctypes.byref(user)):
        raise OSError('cpu_state_unavailable')
    return idle.value,kernel_time.value+user.value

def headroom():
    if os.name!='nt':return False
    kernel=ctypes.windll.kernel32
    m=Memory();m.length=ctypes.sizeof(m)
    if not kernel.GlobalMemoryStatusEx(ctypes.byref(m)) or m.free<max(4*1024**3,m.total*.15):
        return False
    try:
        a,t=system_times(kernel);time.sleep(.15);b,u=system_times(kernel)
        return u>t and (b-a)/(u-t)>=.30
    except OSError:return False
