"""Windows console settings for bridge utilities which launch another console executable."""
import subprocess


def background_console():
    # CREATE_NO_WINDOW leaves a console-less parent. An unmodified grandchild
    # can then allocate a visible terminal. A hidden console is inherited.
    startup = subprocess.STARTUPINFO()
    startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startup.wShowWindow = subprocess.SW_HIDE
    return {"startupinfo": startup, "creationflags": subprocess.CREATE_NEW_CONSOLE}
