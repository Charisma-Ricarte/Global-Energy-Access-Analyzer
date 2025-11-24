# frontend.py (patched and cleaned)
import os
import customtkinter as ctk
from tkinter import ttk, messagebox, StringVar, simpledialog
from PIL import Image, ImageTk
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

import backend

# Appearance
ctk.set_appearance_mode("dark")
WINDOW_W, WINDOW_H = 1150, 700

# Main window
window = ctk.CTk()
window.title("Global Electricity Access Analyzer")
window.geometry(f"{WINDOW_W}x{WINDOW_H}")

# Sidebar + Main frame
sidebar = ctk.CTkFrame(window, width=220, fg_color="#0b0b0b")
sidebar.pack(side="left", fill="y")

main_frame = ctk.CTkFrame(window)
main_frame.pack(side="right", fill="both", expand=True)

# -------------------------------------------------------------------------
# Icon loader
# -------------------------------------------------------------------------
ICON_FOLDER = "icons"

def load_icon(filename, size=(26, 26)):
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
    "query": load_icon("query.png"),
    "view": load_icon("view.png")
}

# -------------------------------------------------------------------------
# Utility helpers
# -------------------------------------------------------------------------
def safe_clear(frame):
    for w in frame.winfo_children():
        w.pack_forget()
        w.grid_forget()
        w.place_forget()
        try:
            w.destroy()
        except Exception:
            pass

def normalize_row(r):
    """
    Normalize a DB row into a dict with keys: id, country, year, population, without, with
    Accepts tuples of varying lengths returned by different schema shapes.
    Ensures numeric fields are ints when possible and None when missing.
    """
    res = {"id": None, "country": None, "year": None, "population": None, "without": None, "with": None}
    if not r:
        return res
    try:
        if len(r) >= 6:
            res["id"], res["country"], res["year"], res["population"], res["without"], res["with"] = r[:6]
        elif len(r) == 5:
            res["id"], res["country"], res["year"], res["without"], res["with"] = r
            res["population"] = None
        elif len(r) == 4:
            res["id"], res["country"], res["year"], res["without"] = r
            res["population"] = None
            res["with"] = None
        else:
            # fallback - best effort
            res["id"] = r[0] if len(r) > 0 else None
            res["country"] = r[1] if len(r) > 1 else None
            res["year"] = r[2] if len(r) > 2 else None
            res["population"] = r[3] if len(r) > 3 else None
    except Exception:
        pass

    # parse numeric-like fields safely
    for k in ("population", "without", "with"):
        v = res.get(k)
        if v is None:
            res[k] = None
            continue
        # If already int/float, keep as int when possible
        if isinstance(v, (int, float)):
            try:
                res[k] = int(v)
            except Exception:
                res[k] = None
            continue
        # If string, clean and parse
        if isinstance(v, str):
            s = v.strip()
            ls = s.lower()
            if ls in ("none", "null", ""):
                res[k] = None
                continue
            # remove commas
            s2 = s.replace(",", "")
            try:
                if "." in s2:
                    val = float(s2)
                    res[k] = int(val)
                else:
                    res[k] = int(s2)
            except Exception:
                try:
                    res[k] = int(float(s2))
                except Exception:
                    res[k] = None
            continue
        # unknown type
        res[k] = None
    return res

def is_aggregate_name(name):
    if not name:
        return False
    n = str(name).strip().lower()
    bad_keywords = [
        "world", "income", "region", "asia", "africa", "americ", "europe",
        "middle", "low", "high", "ida", "ibrd", "blend", "small states",
        "fragile", "pre-demographic", "post-demographic", "demographic",
        "least developed", "heavily indebted", "oecd", "e&c", "europe & central asia",
        "caribbean", "arab world"
    ]
    return any(k in n for k in bad_keywords)

# -------------------------------------------------------------------------
# Table / Chart helpers
# -------------------------------------------------------------------------
def display_table(parent, columns, rows):
    safe_clear(parent)

    header_frame = ctk.CTkFrame(parent)
    header_frame.pack(fill="x", padx=10, pady=(8, 4))

    # record counts
    try:
        num_countries = len(backend.get_countries())
        num_records = len(backend.get_electricity_records())
    except Exception:
        num_countries = "?"
        num_records = "?"

    status_label = ctk.CTkLabel(header_frame, text=f"Countries: {num_countries}    Records: {num_records}", anchor="w")
    status_label.pack(side="left")

    refresh_btn = ctk.CTkButton(header_frame, text="Refresh", width=90, command=screen_view)
    refresh_btn.pack(side="right")

    if not rows:
        msg = ctk.CTkLabel(parent, text=(
            "No data to display.\nIf this persists, check that the loader populated the DB."
        ), wraplength=700, justify="left", font=("Helvetica", 14))
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
    fig, ax = plt.subplots(figsize=(6, 3), dpi=90)
    try:
        ax.plot(x, y, marker="o")
    except Exception:
        xp, yp = [], []
        for xi, yi in zip(x, y):
            try:
                xp.append(float(xi))
                yp.append(float(yi))
            except Exception:
                pass
        ax.plot(xp, yp, marker="o")
    ax.set_title(title)
    ax.set_xlabel("Year")
    ax.set_ylabel("Value")
    ax.grid(True)
    canvas = FigureCanvasTkAgg(fig, master=parent)
    canvas.draw()
    canvas.get_tk_widget().pack(fill="both", expand=True)

# -------------------------------------------------------------------------
# Screens
# -------------------------------------------------------------------------
def screen_view():
    safe_clear(main_frame)
    ctk.CTkLabel(main_frame, text="All Electricity Records", font=("Helvetica", 20)).pack(pady=8)

    # Search
    search_frame = ctk.CTkFrame(main_frame)
    search_frame.pack(fill="x", padx=10, pady=(4, 8))
    ctk.CTkLabel(search_frame, text="Search Country:").pack(side="left", padx=6)
    search_entry = ctk.CTkEntry(search_frame, width=260)
    search_entry.pack(side="left", padx=6)

    def do_search():
        term = search_entry.get().strip().lower()
        try:
            rows = backend.get_electricity_records()
        except Exception as e:
            messagebox.showerror("Error", str(e))
            rows = []
        filtered = []
        for r in rows:
            nr = normalize_row(r)
            cname = nr.get("country") or ""
            if term == "" or term in str(cname).lower():
                filtered.append((nr["id"], nr["country"], nr["year"], nr["without"], nr["with"]))
        display_table(main_frame, ["Record ID", "Country", "Year", "Without Elec", "With Elec"], filtered)

    ctk.CTkButton(search_frame, text="Search", width=100, command=do_search).pack(side="left", padx=8)

    # initial display
    try:
        rows = backend.get_electricity_records()
    except Exception as e:
        messagebox.showerror("Error", str(e))
        rows = []
    out = []
    for r in rows:
        nr = normalize_row(r)
        out.append((nr["id"], nr["country"], nr["year"], nr["without"], nr["with"]))
    display_table(main_frame, ["Record ID", "Country", "Year", "Without Elec", "With Elec"], out)

def screen_add():
    safe_clear(main_frame)
    ctk.CTkLabel(main_frame, text="Add Record", font=("Helvetica", 20)).pack(pady=10)
    frm = ctk.CTkFrame(main_frame)
    frm.pack(pady=15)

    # country dropdown populated from DB
    ctk.CTkLabel(frm, text="Country Name:").grid(row=0, column=0, padx=8, pady=6)
    country_list = [c[1] for c in backend.get_countries()]
    country_combo = ttk.Combobox(frm, values=country_list, width=40)
    country_combo.grid(row=0, column=1)
    if country_list:
        country_combo.set(country_list[0])

    ctk.CTkLabel(frm, text="Year:").grid(row=1, column=0, padx=8, pady=6)
    ent_year = ctk.CTkEntry(frm, width=250)
    ent_year.grid(row=1, column=1)

    ctk.CTkLabel(frm, text="People WITHOUT Electricity:").grid(row=2, column=0, padx=8, pady=6)
    ent_pwo = ctk.CTkEntry(frm, width=250)
    ent_pwo.grid(row=2, column=1)

    ctk.CTkLabel(frm, text="People WITH Electricity (optional):").grid(row=3, column=0, padx=8, pady=6)
    ent_pwi = ctk.CTkEntry(frm, width=250)
    ent_pwi.grid(row=3, column=1)

    def submit():
        cname = country_combo.get().strip()
        if not cname:
            messagebox.showerror("Error", "Country name required.")
            return
        try:
            year = int(ent_year.get())
            pwo = int(ent_pwo.get())
            pwi_val = ent_pwi.get().strip()
            pwi = int(pwi_val) if pwi_val != "" else None
        except Exception:
            messagebox.showerror("Error", "Year and People fields must be numbers.")
            return
        try:
            backend.add_electricity_record(cname, year, pwo, pwi)
            messagebox.showinfo("Success", "Record added.")
            screen_view()
        except Exception as e:
            messagebox.showerror("Error", str(e))

    ctk.CTkButton(main_frame, text="Submit", command=submit).pack(pady=15)

def screen_delete():
    safe_clear(main_frame)
    ctk.CTkLabel(main_frame, text="Delete Record", font=("Helvetica", 20)).pack(pady=10)
    frm = ctk.CTkFrame(main_frame)
    frm.pack(pady=10)
    ctk.CTkLabel(frm, text="Record ID:").grid(row=0, column=0, padx=8, pady=6)
    ent = ctk.CTkEntry(frm, width=200)
    ent.grid(row=0, column=1)

    def do_delete():
        try:
            rid = int(ent.get())
        except Exception:
            messagebox.showerror("Error", "Record ID must be an integer.")
            return
        try:
            backend.delete_electricity_record(rid)
            messagebox.showinfo("Deleted", "Record deleted.")
            screen_view()
        except Exception as e:
            messagebox.showerror("Error", str(e))

    ctk.CTkButton(main_frame, text="Delete", command=do_delete).pack(pady=8)

# -----------------------------
# Queries: use backend-provided functions where possible
# -----------------------------
def show_query_results(rows, cols, title=None, chart=False):
    left = query_left
    right = query_right
    display_table(left, cols, rows)
    if chart and rows:
        try:
            x = [r[0] for r in rows]
            y = [r[1] for r in rows]
            display_chart(right, x, y, title or "")
        except Exception:
            safe_clear(right)

def do_high_unserved():
    try:
        rows = backend.query_high_unserved(1_000_000)
    except Exception as e:
        messagebox.showerror("Query Error", str(e))
        rows = []
    # rows are [(country, total_without), ...] - filter aggregate-like names just in case
    filtered = [(c, v) for (c, v) in rows if not is_aggregate_name(c)]
    show_query_results(filtered, ["Country", "Total Without"], "High Unserved", chart=True)

def do_yearly_trend():
    try:
        rows = backend.query_yearly_trend()  # returns [(year, total_with), ...]
    except Exception as e:
        messagebox.showerror("Query Error", str(e))
        rows = []
    # ensure sorted and numeric
    proc = []
    for y, v in rows:
        try:
            yy = int(y)
        except Exception:
            continue
        try:
            vv = int(v) if v is not None else 0
        except Exception:
            vv = 0
        proc.append((yy, vv))
    proc.sort(key=lambda x: x[0])
    show_query_results(proc, ["Year", "With Electricity"], "Yearly Trend", chart=True)

def ask_year_and_run(fn):
    # ask the user for a year using a modal dialog, then run fn(year)
    try:
        y = simpledialog.askinteger("Select Year", "Year (e.g. 2015):", parent=window, minvalue=1900, maxvalue=2100)
    except Exception:
        y = None
    if y is None:
        return
    try:
        rows = fn(y)
    except Exception as e:
        messagebox.showerror("Query Error", str(e))
        return
    return rows

def do_access_percent():
    rows = ask_year_and_run(backend.query_access_percent)
    if not rows:
        return
    # rows are [(country, population, without, access_pct), ...]
    filtered = []
    for country, pop, without, pct in rows:
        if not country or is_aggregate_name(country):
            continue
        # ensure correct types
        try:
            p = int(pop) if pop is not None else None
        except Exception:
            p = None
        try:
            w = int(without) if without is not None else 0
        except Exception:
            w = 0
        filtered.append((country, p if p is not None else "None", w, pct if pct is not None else "None"))
    show_query_results(filtered, ["Country", "Population", "Without", "Access %"], "Access % by Country")

def do_most_improved():
    # ask user for start and end years optionally
    start = simpledialog.askinteger("Start year", "Start year (default 1990):", parent=window, minvalue=1900, maxvalue=2100)
    if start is None:
        start = 1990
    end = simpledialog.askinteger("End year", "End year (default 2016):", parent=window, minvalue=1900, maxvalue=2100)
    if end is None:
        end = 2016
    try:
        rows = backend.query_most_improved(start, end)
    except Exception as e:
        messagebox.showerror("Query Error", str(e))
        rows = []
    # rows are [(country, start_access, end_access, improvement), ...]
    proc = []
    for c, a0, a1, imp in rows:
        try:
            a0v = float(a0) if a0 is not None else None
        except Exception:
            a0v = None
        try:
            a1v = float(a1) if a1 is not None else None
        except Exception:
            a1v = None
        try:
            impv = float(imp) if imp is not None else None
        except Exception:
            impv = None
        proc.append((c, a0v if a0v is not None else "None", a1v if a1v is not None else "None", impv if impv is not None else "None"))
    show_query_results(proc, ["Country", f"Access{start}", f"Access{end}", "Improvement"], "Most Improved")

# Regional Compare screen & helpers (year selector shown only here)
def screen_regional_compare():
    safe_clear(main_frame)
    ctk.CTkLabel(main_frame, text="Regional Compare", font=("Helvetica", 20)).pack(pady=6)

    ctrl = ctk.CTkFrame(main_frame, fg_color="#101010")
    ctrl.pack(fill="x", padx=8, pady=(6, 4))

    ctk.CTkLabel(ctrl, text="Select year for Regional Compare / Compare Two Countries:").pack(side="left", padx=6)
    year_var = StringVar(value="2015")
    years = [str(y) for y in range(1990, 2017)]
    year_combo = ttk.Combobox(ctrl, values=years, textvariable=year_var, width=8)
    year_combo.pack(side="left", padx=6)
    year_combo.set("2015")

    # layout: left for table, right for chart
    container = ctk.CTkFrame(main_frame)
    container.pack(fill="both", expand=True, padx=8, pady=8)
    global query_left, query_right
    query_left = ctk.CTkFrame(container); query_left.pack(side="left", fill="both", expand=True, padx=6, pady=6)
    query_right = ctk.CTkFrame(container); query_right.pack(side="right", fill="both", expand=True, padx=6, pady=6)

    def run_region():
        try:
            y = int(year_combo.get())
        except Exception:
            messagebox.showerror("Error", "Year must be an integer.")
            return
        try:
            rows = backend.query_regional_comparison(y)
        except Exception as e:
            messagebox.showerror("Query Error", str(e))
            rows = []
        # rows are [(region, access_pct)]
        filtered = [(r, pct) for (r, pct) in rows if r and not is_aggregate_name(r)]
        show_query_results(filtered, ["Region", "Access %"], "Regional Compare", chart=True)

    def open_compare_popup():
        popup = ctk.CTkToplevel(window)
        popup.title("Compare Two Countries")
        popup.geometry("480x340")
        ctk.CTkLabel(popup, text="Year:").pack(pady=6)
        y_combo = ttk.Combobox(popup, values=years, width=10)
        y_combo.set(year_combo.get())
        y_combo.pack(padx=10)

        ctk.CTkLabel(popup, text="Country A:").pack(pady=6)
        country_list = [c[1] for c in backend.get_countries() if c and c[1]]
        combo_a = ttk.Combobox(popup, values=country_list, width=40)
        combo_a.set(country_list[0] if country_list else "")
        combo_a.pack(padx=10)

        ctk.CTkLabel(popup, text="Country B:").pack(pady=6)
        combo_b = ttk.Combobox(popup, values=country_list, width=40)
        combo_b.set(country_list[1] if len(country_list) > 1 else (country_list[0] if country_list else ""))
        combo_b.pack(padx=10)

        def run_compare():
            try:
                yr = int(y_combo.get())
            except Exception:
                messagebox.showerror("Error", "Year must be an integer.")
                return
            a = combo_a.get().strip()
            b = combo_b.get().strip()
            if not a or not b:
                messagebox.showerror("Error", "Choose both countries.")
                return
            try:
                rows = backend.query_two_country_compare(yr, a, b)
            except Exception as e:
                messagebox.showerror("Query Error", str(e))
                return
            # rows: list of tuples (country, access_pct, population, with_people, without_people)
            show_query_results(rows, ["Country","Access %","Population","With","Without"], title=f"Compare: {a} vs {b}")
            popup.destroy()

        ctk.CTkButton(popup, text="Compare", command=run_compare).pack(pady=12)

    # Buttons area
    btns = ctk.CTkFrame(main_frame)
    btns.pack(pady=6)
    ctk.CTkButton(btns, text="Run Regional Compare", command=run_region).pack(side="left", padx=6)
    ctk.CTkButton(btns, text="Compare Two Countries", command=open_compare_popup).pack(side="left", padx=6)
    # run initial region
    run_region()

# -----------------------------
# Queries screen wiring
# -----------------------------
def screen_queries():
    safe_clear(main_frame)
    ctk.CTkLabel(main_frame, text="Analytical Queries", font=("Helvetica", 20)).pack(pady=6)

    desc_panel = ctk.CTkFrame(main_frame, fg_color="#101010")
    desc_panel.pack(fill="x", padx=8, pady=(6, 4))
    desc_label = ctk.CTkLabel(desc_panel, text="Select a query below.", anchor="w")
    desc_label.pack(fill="x", padx=12, pady=8)

    container = ctk.CTkFrame(main_frame)
    container.pack(fill="both", expand=True, padx=8, pady=8)
    global query_left, query_right
    query_left = ctk.CTkFrame(container); query_left.pack(side="left", fill="both", expand=True, padx=6, pady=6)
    query_right = ctk.CTkFrame(container); query_right.pack(side="right", fill="both", expand=True, padx=6, pady=6)

    # Buttons for queries - simple and explicit
    btn_frame = ctk.CTkFrame(main_frame)
    btn_frame.pack(pady=6)

    b1 = ctk.CTkButton(btn_frame, text="High Unserved", image=icons.get("query"), width=170, command=lambda: (desc_label.configure(text="Countries with >1M without electricity."), do_high_unserved()))
    b2 = ctk.CTkButton(btn_frame, text="Yearly Trend", image=icons.get("query"), width=170, command=lambda: (desc_label.configure(text="Global people WITH electricity (by year)."), do_yearly_trend()))
    b3 = ctk.CTkButton(btn_frame, text="Access % (Year)", image=icons.get("query"), width=170, command=lambda: (desc_label.configure(text="Percent with electricity for selected year."), do_access_percent()))
    b4 = ctk.CTkButton(btn_frame, text="Most Improved", image=icons.get("query"), width=170, command=lambda: (desc_label.configure(text="Countries with biggest improvement between two years."), do_most_improved()))
    b5 = ctk.CTkButton(btn_frame, text="Regional Compare", image=icons.get("query"), width=170, command=lambda: (desc_label.configure(text="Regional electricity access comparison (select year in regional screen)."), screen_regional_compare()))

    b1.grid(row=0, column=0, padx=8, pady=6)
    b2.grid(row=0, column=1, padx=8, pady=6)
    b3.grid(row=0, column=2, padx=8, pady=6)
    b4.grid(row=1, column=0, padx=8, pady=6)
    b5.grid(row=1, column=1, padx=8, pady=6)

    # initial empty content
    safe_clear(query_left)
    safe_clear(query_right)
    ctk.CTkLabel(query_left, text="Select a query to view results.", anchor="w").pack(padx=12, pady=12)

# -------------------------------------------------------------------------
# Sidebar navigation
# -------------------------------------------------------------------------
def add_nav_button(text, cmd, y, icon=None):
    if icon:
        btn = ctk.CTkButton(sidebar, text=text, image=icon, width=200, height=50, anchor="w", command=cmd)
    else:
        btn = ctk.CTkButton(sidebar, text=text, width=200, height=50, anchor="w", command=cmd)
    btn.place(x=10, y=y)

add_nav_button("View Records", screen_view, 120, icons.get("view"))
add_nav_button("Add Record", screen_add, 190, icons.get("add"))
add_nav_button("Delete Record", screen_delete, 260, icons.get("delete"))
add_nav_button("Queries", screen_queries, 330, icons.get("query"))

# Start
screen_view()

# Ensure a graceful exit to reduce "after" callbacks on some systems
def on_close():
    try:
        window.quit()
    except Exception:
        pass
    try:
        window.destroy()
    except Exception:
        pass

window.protocol("WM_DELETE_WINDOW", on_close)
window.mainloop()
