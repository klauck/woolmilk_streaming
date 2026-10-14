"""Thin helpers for running commands over SSH.

Everything runs non-interactively, so every ssh/scp call gets stdin=DEVNULL --
otherwise ssh steals the terminal's stdin (e.g. the kill confirmation prompt).
"""
import subprocess


def run_ssh(cmd, **kwargs):
    kwargs.setdefault("stdin", subprocess.DEVNULL)
    return subprocess.run(cmd, **kwargs)


def on_exec_host(mode, jump, command):
    """Wrap a shell command to run on the executing host: this machine, or the jump."""
    if mode == "jump":
        return ["ssh", jump, command]
    return ["sh", "-c", command]


def quote(s):
    return "'" + s.replace("'", "'\\''") + "'"
