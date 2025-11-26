# frontend.py
# Final GUI: uses backend.py API, icons from icons/, query description panel above the table (option B).

import os
import customtkinter as ctk
from tkinter import ttk, messagebox
from PIL import Image, ImageTk
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

import backend

# Appearance
ctk.set_appearance_mode("dark")
window_width = 1000
window_height = 500

window = ctk.CTk()
window.title("Global Electricity Access Analyzer")
screen_width = window.winfo_screenwidth()
screen_height = window.winfo_screenheight()
x = int((screen_width / 2) - (window_width / 2))
y = int((screen_height / 2) - (window_height / 2))
window.geometry(f'{window_width}x{window_height}+{x}+{y}')

# Sidebar + main frame
sidebar = ctk.CTkFrame(window, width=220, fg_color="#0b0b0b")
sidebar.pack(side="left", fill="y")

main_frame = ctk.CTkFrame(window)
main_frame.pack(side="right", fill="both", expand=True)

# ---------------------------------------
# Icon loader (graceful if missing)
# ---------------------------------------
ICON_FOLDER = "icons"

def load_icon(filename, size=(26,26)):
    path = os.path.join(ICON_FOLDER, filename)
    try:
        img = Image.open(path)
        return ctk.CTkImage(light_image=img, size=size)
    except Exception:
        return None

icons = {
    "add": load_icon("add.png"),
    "back": load_icon("back.png"),
    "delete": load_icon("delete.png"),
    "query": load_icon("query.pny") or load_icon("query.png"),
    "view": load_icon("view.png")
}

# ---------------------------------------
# Safe clearing helpers
# ---------------------------------------
def safe_clear(frame):
    for w in frame.winfo_children():
        w.pack_forget()
        w.grid_forget()
        w.place_forget()

def display_table(parent, columns, rows):
    """Displays a table. If rows empty, show helpful message with counts and Refresh button."""
    safe_clear(parent)

    header_frame = ctk.CTkFrame(parent)
    header_frame.pack(fill="x", padx=10, pady=(8,4))

    # show counts from backend for debugging
    try:
        countries = backend.get_countries()
        num_countries = len(countries)
    except Exception:
        num_countries = "?"

    try:
        full_rows = backend.get_electricity_records()
        num_records = len(full_rows)
    except Exception:
        num_records = "?"

    status_label = ctk.CTkLabel(header_frame, text=f"Countries: {num_countries}    Records: {num_records}", anchor="w")
    status_label.pack(side="left")

    refresh_btn = ctk.CTkButton(header_frame, text="Refresh", width=90, command=screen_view)
    refresh_btn.pack(side="right")

    if not rows:
        # empty - show helpful text and return
        msg = ctk.CTkLabel(parent, text="No data to display.\nIf this persists, check that the loader has populated the DB and that backend.get_electricity_records() returns rows.",
                           wraplength=700, justify="left", font=("Helvetica", 14))
        msg.pack(padx=20, pady=30)
        return

    container = ctk.CTkFrame(parent)
    container.pack(fill="both", expand=True, padx=10, pady=10)

    tree = ttk.Treeview(container, columns=columns, show="headings", height=18)
    for c in columns:
        tree.heading(c, text=c)
        tree.column(c, width=140, anchor="center")

    for r in rows:
        safe_row = tuple("" if v is None else v for v in r)
        tree.insert("", "end", values=safe_row)

    vsb = ttk.Scrollbar(container, orient="vertical", command=tree.yview)
    tree.configure(yscrollcommand=vsb.set)

    tree.pack(side="left", fill="both", expand=True)
    vsb.pack(side="right", fill="y")

def display_chart(parent, x, y, title):
    safe_clear(parent)
    fig, ax = plt.subplots(figsize=(5,3), dpi=90)
    try:
        ax.plot(x, y)
    except Exception:
        xp, yp = [], []
        for xi, yi in zip(x, y):
            try:
                xp.append(float(xi))
                yp.append(float(yi))
            except Exception:
                pass
        ax.plot(xp, yp)
    ax.set_title(title)
    ax.set_xlabel("Year")
    ax.set_ylabel("Value")
    ax.grid(True)
    canvas = FigureCanvasTkAgg(fig, master=parent)
    canvas.draw()
    canvas.get_tk_widget().pack(fill="both", expand=True)

# ---------------------------------------
# Screens
# ---------------------------------------

def screen_view():
    safe_clear(main_frame)
    heading = ctk.CTkLabel(main_frame, text="All Electricity Records", font=("Helvetica", 20))
    heading.pack(pady=8)

    try:
        rows = backend.get_electricity_records()
    except Exception as e:
        messagebox.showerror("Backend error", f"Error calling backend.get_electricity_records():\n{e}")
        rows = []

    cols = ["Record ID", "Country", "Year", "Without Elec", "With Elec"]
    display_table(main_frame, cols, rows)

def screen_add():
    safe_clear(main_frame)
    
    ctk.CTkLabel(
        main_frame,
        text="Add Record",
        font=("Helvetica", 20)
    ).pack(pady=60)

    frm = ctk.CTkFrame(main_frame)
    frm.pack(pady=10)

    ctk.CTkLabel(frm, text="Country Name:").grid(row=0, column=0, padx=8, pady=6, sticky="e")
    ent_cid = ctk.CTkEntry(frm, width=100)
    ent_cid.grid(row=0, column=1)

    ctk.CTkLabel(frm, text="Year:").grid(row=1, column=0, padx=8, pady=6, sticky="e")
    ent_year = ctk.CTkEntry(frm, width=100)
    ent_year.grid(row=1, column=1)

    ctk.CTkLabel(frm, text="People Without:").grid(row=2, column=0, padx=8, pady=6, sticky="e")
    ent_pwe = ctk.CTkEntry(frm, width=100)
    ent_pwe.grid(row=2, column=1)

    ctk.CTkLabel(frm, text="People With:").grid(row=3, column=0, padx=8, pady=6, sticky="e")
    ent_p = ctk.CTkEntry(frm, width=100)
    ent_p.grid(row=3, column=1)

    def submit():
        try:
            backend.add_electricity_record(
                int(ent_cid.get()),
                int(ent_year.get()),
                int(ent_pwe.get())
            )
            messagebox.showinfo("Success", "Record added")
            screen_view()
        except Exception as e:
            messagebox.showerror("Error", str(e))
    
    # frame for submit button
    ctk.CTkButton(frm, text="Submit", command=submit, height=30).grid(row=4, column=0, columnspan=2, pady=(12, 20))

def screen_delete():
    safe_clear(main_frame)

    ctk.CTkLabel(main_frame, text="Delete Record", font=("Helvetica", 20)).pack(pady=60)
    frm = ctk.CTkFrame(main_frame)
    frm.pack(pady=10)

    ctk.CTkLabel(frm, text="Record ID:").grid(row=0, column=0, padx=8, pady=6)
    ent_id = ctk.CTkEntry(frm, width=50)
    ent_id.grid(row=0, column=1)

    def do_del():
        try:
            backend.delete_electricity_record(int(ent_id.get()))
            messagebox.showinfo("Deleted","Record removed")
            screen_view()
        except Exception as e:
            messagebox.showerror("Error", str(e))

    ctk.CTkButton(frm, text="Delete", command=do_del, width=80).grid(row=1, column=0, columnspan=2, pady=(12, 20))

def screen_queries():
    safe_clear(main_frame)
    ctk.CTkLabel(main_frame, text="Analytical Queries", font=("Helvetica",20)).pack(pady=6)

    # DESCRIPTION panel above the table (option B)
    desc_panel = ctk.CTkFrame(main_frame, fg_color="#101010")
    desc_panel.pack(fill="x", padx=8, pady=(6,4))
    desc_label = ctk.CTkLabel(desc_panel, text="Select a query below — a short description will appear here.", anchor="w")
    desc_label.pack(fill="x", padx=12, pady=8)

    container = ctk.CTkFrame(main_frame)
    container.pack(fill="both", expand=True, padx=8, pady=8)
    left = ctk.CTkFrame(container); left.pack(side="left", fill="both", expand=True, padx=6, pady=6)
    right = ctk.CTkFrame(container); right.pack(side="right", fill="both", expand=True, padx=6, pady=6)

    def show_result(rows, cols, chart_title=None):
        display_table(left, cols, rows)
        if chart_title and rows:
            try:
                x = [r[0] for r in rows]
                y = [r[1] for r in rows]
                display_chart(right, x, y, chart_title)
            except Exception:
                safe_clear(right)

    queries = [
        ("High Unserved", "Countries with >10M people without electricity.", lambda: backend.query_high_unserved(10_000_000), ["Country","Year","Without Elec"], None),
        ("Yearly Trend", "Total people WITH electricity each year (global).", backend.query_yearly_trend, ["Year","With Elec"], "Yearly Electricity Trend"),
        ("Access % (2015)", "Percent of population with electricity in 2015 (countries with population data).", lambda: backend.query_access_percent(2015), ["Country","Access %"], None),
        ("Regional Compare (2015)", "Average electricity access by region for 2015.", lambda: backend.query_regional_comparison(2015), ["Region","Access %"], None),
        ("Most Improved", "Countries with the biggest drop in people without electricity (improvement).", backend.query_most_improved, ["Country","Improvement"], None)
    ]

    btn_frame = ctk.CTkFrame(main_frame)
    btn_frame.pack(pady=6)

    for i, (label, desc, fn, cols, chart_title) in enumerate(queries):
        def make_cb(fn=fn, d=desc, cols=cols, title=chart_title):
            def cb():
                desc_label.configure(text=d)
                try:
                    rows = fn() if callable(fn) else fn
                except Exception as e:
                    messagebox.showerror("Query error", str(e))
                    rows = []
                show_result(rows, cols, title)
            return cb

        r = i // 3
        c = i % 3
        if icons.get("query"):
            btn = ctk.CTkButton(btn_frame, text=label, image=icons.get("query"), width=170, command=make_cb())
        else:
            btn = ctk.CTkButton(btn_frame, text=label, width=170, command=make_cb())
        btn.grid(row=r, column=c, padx=8, pady=6)


# ---------------------------------------
# Sidebar buttons (with icons if available)
# ---------------------------------------
def add_nav_button(text, cmd, y, icon=None):
    if icon:
        btn = ctk.CTkButton(sidebar, text=text, image=icon, width=200, height=50, anchor="w", command=cmd)
    else:
        btn = ctk.CTkButton(sidebar, text=text, width=200, height=50, anchor="w", command=cmd)
    btn.place(x=10, y=y)

add_nav_button("View Records", screen_view, 5, icons.get("view"))
add_nav_button("Add Record", screen_add, 60, icons.get("add"))
add_nav_button("Delete Record", screen_delete, 115, icons.get("delete"))
add_nav_button("Queries", screen_queries, 170, icons.get("query"))

# start with view
screen_view()

window.mainloop()
