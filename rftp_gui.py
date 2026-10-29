"""
rftp_gui.py - Tkinter GUI for RFTP (file selection, progress bar, live log, statistics).
Run:  python rftp_gui.py
Tab 1 = Receiver (server), Tab 2 = Sender (client). Run two copies (or one on each PC).
"""
import threading
import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext, messagebox

from rftp_server import RFTPServer
from rftp_client import send_file


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("RFTP - Reliable File Transfer Protocol over UDP")
        self.geometry("760x640")
        self.server = None
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True, padx=6, pady=6)
        self.srv_tab, self.cli_tab = ttk.Frame(nb), ttk.Frame(nb)
        nb.add(self.srv_tab, text="  Receiver (Server)  ")
        nb.add(self.cli_tab, text="  Sender (Client)  ")
        self._build_server()
        self._build_client()

    # ------------------------------------------------------------ helpers
    def _row(self, parent, label, default, r, width=28):
        ttk.Label(parent, text=label).grid(row=r, column=0, sticky="w", padx=6, pady=3)
        v = tk.StringVar(value=default)
        ttk.Entry(parent, textvariable=v, width=width).grid(row=r, column=1, sticky="w", pady=3)
        return v

    def _log(self, box, msg):
        self.after(0, lambda: (box.insert("end", msg + "\n"), box.see("end")))

    # ------------------------------------------------------------- server
    def _build_server(self):
        f = self.srv_tab
        self.s_port = self._row(f, "Listen port", "9000", 0)
        self.s_out = self._row(f, "Save folder", "received", 1)
        ttk.Button(f, text="Browse...", command=lambda: self.s_out.set(
            filedialog.askdirectory() or self.s_out.get())).grid(row=1, column=2)
        self.s_loss = self._row(f, "ACK loss (0-1)", "0", 2)
        self.s_btn = ttk.Button(f, text="Start Server", command=self.toggle_server)
        self.s_btn.grid(row=3, column=0, columnspan=2, pady=8)
        self.s_log = scrolledtext.ScrolledText(f, height=26)
        self.s_log.grid(row=4, column=0, columnspan=3, sticky="nsew", padx=6)
        f.rowconfigure(4, weight=1); f.columnconfigure(2, weight=1)

    def toggle_server(self):
        if self.server:
            self.server.stop(); self.server = None
            self.s_btn.config(text="Start Server"); self._log(self.s_log, "[SERVER] stopped"); return
        try:
            self.server = RFTPServer("0.0.0.0", int(self.s_port.get()), self.s_out.get(),
                                     loss=float(self.s_loss.get()),
                                     log=lambda m: self._log(self.s_log, m))
        except Exception as e:
            messagebox.showerror("Server error", str(e)); return
        self.server.start()
        self.s_btn.config(text="Stop Server")

    # ------------------------------------------------------------- client
    def _build_client(self):
        f = self.cli_tab
        self.c_file = self._row(f, "File to send", "", 0, 40)
        ttk.Button(f, text="Browse...", command=lambda: self.c_file.set(
            filedialog.askopenfilename() or self.c_file.get())).grid(row=0, column=2)
        self.c_host = self._row(f, "Server IP", "127.0.0.1", 1)
        self.c_port = self._row(f, "Server port", "9000", 2)
        self.c_loss = self._row(f, "Packet loss (0-1)", "0.1", 3)
        self.c_corr = self._row(f, "Corruption (0-1)", "0", 4)
        self.c_delay = self._row(f, "Max delay (s)", "0", 5)
        self.c_to = self._row(f, "ACK timeout (s)", "0.3", 6)
        self.c_rel = tk.BooleanVar(value=True)
        ttk.Checkbutton(f, text="Use RFTP reliability (untick = plain UDP baseline)",
                        variable=self.c_rel).grid(row=7, column=0, columnspan=3, sticky="w", padx=6)
        self.c_btn = ttk.Button(f, text="Send File", command=self.send)
        self.c_btn.grid(row=8, column=0, columnspan=2, pady=6)
        self.prog = ttk.Progressbar(f, length=500); self.prog.grid(row=9, column=0, columnspan=3, padx=6, sticky="we")
        self.c_log = scrolledtext.ScrolledText(f, height=18)
        self.c_log.grid(row=10, column=0, columnspan=3, sticky="nsew", padx=6, pady=4)
        f.rowconfigure(10, weight=1); f.columnconfigure(2, weight=1)

    def send(self):
        if not self.c_file.get():
            messagebox.showwarning("No file", "Choose a file first."); return
        self.c_btn.config(state="disabled"); self.prog["value"] = 0

        def progress(done, total):
            self.after(0, lambda: self.prog.config(value=100 * done / max(total, 1)))

        def work():
            try:
                send_file(self.c_file.get(), self.c_host.get(), int(self.c_port.get()),
                          timeout=float(self.c_to.get()), loss=float(self.c_loss.get()),
                          corrupt=float(self.c_corr.get()), delay=float(self.c_delay.get()),
                          reliable=self.c_rel.get(), on_progress=progress,
                          log=lambda m: self._log(self.c_log, m))
            except Exception as e:
                self._log(self.c_log, f"[CLIENT] ERROR: {e}")
            self.after(0, lambda: self.c_btn.config(state="normal"))
        threading.Thread(target=work, daemon=True).start()


if __name__ == "__main__":
    App().mainloop()
