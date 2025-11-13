# frontend.py
import customtkinter as ctk
from tkinter import messagebox
import backend

ctk.set_appearance_mode("dark")

window = ctk.CTk()
window.title("Global Electricity Access Analyzer")
window.geometry("800x500")

# ----------------------------------------------------------
# Helpers
# ----------------------------------------------------------

def clear_screen():
    for widget in window.winfo_children():
        widget.place_forget()

def back_to_menu():
    clear_screen()
    build_menu()

# ----------------------------------------------------------
# ADD RECORD SCREEN
# ----------------------------------------------------------

def add_screen():
    clear_screen()

    ctk.CTkLabel(window, text="Add Electricity Record", font=("Helvetica", 20)).place(x=260, y=40)

    # Inputs
    country_id = ctk.CTkEntry(window, width=120)
    year = ctk.CTkEntry(window, width=120)
    pwe = ctk.CTkEntry(window, width=120)

    ctk.CTkLabel(window, text="Country ID").place(x=250, y=120)
    country_id.place(x=380, y=120)

    ctk.CTkLabel(window, text="Year").place(x=250, y=160)
    year.place(x=380, y=160)

    ctk.CTkLabel(window, text="People Without Electricity").place(x=250, y=200)
    pwe.place(x=380, y=200)

    def submit():
        try:
            backend.add_electricity_record(
                int(country_id.get()),
                int(year.get()),
                int(pwe.get())
            )
            messagebox.showinfo("Success", "Record added!")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    ctk.CTkButton(window, text="Submit", command=submit).place(x=330, y=260)
    ctk.CTkButton(window, text="Back", command=back_to_menu).place(x=330, y=300)

# ----------------------------------------------------------
# VIEW RECORDS SCREEN
# ----------------------------------------------------------

def view_screen():
    clear_screen()

    ctk.CTkLabel(window, text="All Records", font=("Helvetica", 20)).place(x=320, y=30)

    records = backend.get_electricity_records()

    text = ctk.CTkTextbox(window, width=700, height=350)
    text.place(x=50, y=80)

    for r in records:
        text.insert("end", str(r) + "\n")

    ctk.CTkButton(window, text="Back", command=back_to_menu).place(x=350, y=450)

# ----------------------------------------------------------
# DELETE SCREEN
# ----------------------------------------------------------

def delete_screen():
    clear_screen()

    ctk.CTkLabel(window, text="Delete Record", font=("Helvetica", 20)).place(x=300, y=40)

    rec_id = ctk.CTkEntry(window, width=120)
    ctk.CTkLabel(window, text="Record ID").place(x=270, y=150)
    rec_id.place(x=360, y=150)

    def delete():
        try:
            backend.delete_electricity_record(int(rec_id.get()))
            messagebox.showinfo("Success", "Record Deleted")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    ctk.CTkButton(window, text="Delete", command=delete).place(x=350, y=220)
    ctk.CTkButton(window, text="Back", command=back_to_menu).place(x=350, y=260)

# ----------------------------------------------------------
# ANALYTICAL QUERY SCREEN
# ----------------------------------------------------------

def query_screen():
    clear_screen()

    ctk.CTkLabel(window, text="Analytical Queries", font=("Helvetica", 20)).place(x=290, y=40)

    def show_result(data, title):
        result = ctk.CTkTextbox(window, width=700, height=300)
        result.place(x=50, y=180)
        result.insert("end", title + "\n\n")
        for row in data:
            result.insert("end", str(row) + "\n")

    # Query: High Unserved
    def q1():
        data = backend.query_high_unserved(10000000)
        show_result(data, "Countries w/ >10M Unserved")

    # Query: Trend
    def q2():
        data = backend.query_yearly_trend()
        show_result(data, "Yearly Global Electricity Trend")

    # Query: Access Percent
    def q3():
        data = backend.query_access_percent(2015)
        show_result(data, "Access Percent (2015)")

    # Query: Regional Comparison
    def q4():
        data = backend.query_regional_comparison(2015)
        show_result(data, "Regional Comparison (2015)")

    # Query: Most Improved
    def q5():
        data = backend.query_most_improved()
        show_result(data, "Most Improved Countries")

    # Buttons
    ctk.CTkButton(window, text="High Unserved", command=q1).place(x=150, y=120)
    ctk.CTkButton(window, text="Trend", command=q2).place(x=350, y=120)
    ctk.CTkButton(window, text="Access %", command=q3).place(x=550, y=120)

    ctk.CTkButton(window, text="Regional Compare", command=q4).place(x=250, y=160)
    ctk.CTkButton(window, text="Most Improved", command=q5).place(x=450, y=160)

    ctk.CTkButton(window, text="Back", command=back_to_menu).place(x=350, y=500)

# ----------------------------------------------------------
# MAIN MENU
# ----------------------------------------------------------

def build_menu():
    ctk.CTkLabel(window, text="Electricity Database Manager", font=("Helvetica", 24)).place(x=240, y=50)

    ctk.CTkButton(window, text="Add Record", width=200, command=add_screen).place(x=100, y=200)
    ctk.CTkButton(window, text="View Records", width=200, command=view_screen).place(x=300, y=200)
    ctk.CTkButton(window, text="Delete Record", width=200, command=delete_screen).place(x=500, y=200)
    ctk.CTkButton(window, text="Run Queries", width=200, command=query_screen).place(x=300, y=260)

build_menu()
window.mainloop()
