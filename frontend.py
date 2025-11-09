import customtkinter as ctk
from PIL import Image

# set up window size, position
window_width = 700
window_height = 400
window = ctk.CTk()
window.title("Electricity Database Management")
screen_width = window.winfo_screenwidth()
screen_height = window.winfo_screenheight()
x = int((screen_width / 2) - (window_width / 2))
y = int((screen_height / 2) - (window_height / 2))
window.geometry(f'{window_width}x{window_height}+{x}+{y}')
window.resizable(False, False)
window.iconbitmap("icon.ico")

# background image
bg_image = ctk.CTkImage(dark_image=Image.open("background.png"), size=(700, 400))
bg_label = ctk.CTkLabel(window, image=bg_image, text="")
bg_label.place(x=0, y=0, relwidth=1, relheight=1)
bg_label.lower()
ctk.set_appearance_mode("dark")
window.configure(fg_color="#000000")

def hide_all_buttons():
    clear_screen(exceptions=[bg_label])
    back_button.place(x=600, y=15)

# clear everything in the window except background
def clear_screen(exceptions=()):
    for w in window.winfo_children():
        if w in exceptions:
            continue
        w.place_forget()

# define button event

def back_button_event():
    clear_screen(exceptions=[bg_label])
    add_button.place(x=150, y=100)
    edit_button.place(x=400, y=100)
    delete_button.place(x=150, y=190)
    view_button.place(x=400, y=190)
    back_button.place_forget()

def confirm_btn():
    code = country_code.get()
    name = country_name.get()
    ppl = people.get()
    yr = year.get()
    if not (code and name and ppl and yr):
        status_label.place(x=270, y=350)
        status_label.configure(text="Please fill all information", text_color='#e63946')
    else:
        status_label.place(x=320, y=350)
        status_label.configure(text="Successful!", text_color="#6a994e")

def show_add_screen():
    hide_all_buttons()
    global country_code, country_name, year, people, status_label

    country_c = ctk.CTkLabel(window, text="Country Code", font=("Helvetica", 14, "bold"))
    country_c.place(x=215, y=80)
    country_code = ctk.CTkEntry(window, width=60, fg_color= '#e5e5e5', text_color="#000000")
    country_code.place(x=350, y=80)

    country_n = ctk.CTkLabel(window, text="Country Name", font=("Helvetica", 14, "bold"))
    country_n.place(x=215, y=120)
    country_name = ctk.CTkEntry(window, width=180, fg_color= '#e5e5e5', text_color="#000000")
    country_name.place(x=350, y=120)

    n_people = ctk.CTkLabel(window, text="Number of People", font=("Helvetica", 14, "bold"))
    n_people.place(x=215, y=160)
    people = ctk.CTkEntry(window, width=120, fg_color= '#e5e5e5', text_color="#000000")
    people.place(x=350, y=160)

    yr = ctk.CTkLabel(window, text="Year", font=("Helvetica", 14, "bold"))
    yr.place(x=215, y=200)
    year = ctk.CTkEntry(window, width=80, fg_color= '#e5e5e5', text_color="#000000")
    year.place(x=350, y=200)

    status_label = ctk.CTkLabel(window, text="", font=("Helvetica", 14, "bold"))
    
    add_confirm = ctk.CTkButton(window, text="ADD", font=("Helvetica", 14, "bold"), width=120, height=40, fg_color="#588157", hover_color="#436644", cursor="hand2", command=confirm_btn)
    add_confirm.place(x=300, y=300)

def show_edit_screen():
    hide_all_buttons()
    global country_code, country_name, year, people, status_label

    country_c = ctk.CTkLabel(window, text="Country Code", font=("Helvetica", 14, "bold"))
    country_c.place(x=215, y=80)
    country_code = ctk.CTkEntry(window, width=60, fg_color= '#e5e5e5', text_color="#000000")
    country_code.place(x=350, y=80)

    country_n = ctk.CTkLabel(window, text="Country Name", font=("Helvetica", 14, "bold"))
    country_n.place(x=215, y=120)
    country_name = ctk.CTkEntry(window, width=180, fg_color= '#e5e5e5', text_color="#000000")
    country_name.place(x=350, y=120)

    n_ppl = ctk.CTkLabel(window, text="Number of People", font=("Helvetica", 14, "bold"))
    n_ppl.place(x=215, y=160)
    people = ctk.CTkEntry(window, width=120, fg_color= '#e5e5e5', text_color="#000000")
    people.place(x=350, y=160)

    yr = ctk.CTkLabel(window, text="Year", font=("Helvetica", 14, "bold"))
    yr.place(x=215, y=200)
    year = ctk.CTkEntry(window, width=80, fg_color= '#e5e5e5', text_color="#000000")
    year.place(x=350, y=200)

    status_label = ctk.CTkLabel(window, text="", font=("Helvetica", 14, "bold"))
    
    save_btn = ctk.CTkButton(window, text="SAVE", font=("Helvetica", 14, "bold"), width=120, height=40, fg_color="#0077b6", hover_color="#025b87", cursor="hand2", command=confirm_btn)
    save_btn.place(x=300, y=300)

def show_view_screen():
    hide_all_buttons()
    global country_code, country_name, year, people, status_label

    country_c = ctk.CTkLabel(window, text="Country Code", font=("Helvetica", 14, "bold"))
    country_c.place(x=215, y=120)
    country_c2 = ctk.CTkEntry(window, width=60, fg_color= '#e5e5e5', text_color="#000000")
    country_c2.place(x=320, y=120)

    label = ctk.CTkLabel(window, text="OR", font=("Helvetica", 14, "bold"))
    label.place(x=330, y=160)

    country_n = ctk.CTkLabel(window, text="Country Name", font=("Helvetica", 14, "bold"))
    country_n.place(x=215, y=200)
    country_n2 = ctk.CTkEntry(window, width=180, fg_color= '#e5e5e5', text_color="#000000")
    country_n2.place(x=320, y=200)

    search_btn = ctk.CTkButton(window, text="SEARCH", font=("Helvetica", 14, "bold"), width=120, height=40, fg_color="#778da9", hover_color="#415a77", cursor="hand2")
    search_btn.place(x=300, y=300)

def show_delete_screen():
    global country_code, country_name, year, status_label
    hide_all_buttons()

    country_c = ctk.CTkLabel(window, text="Country Code", font=("Helvetica", 14, "bold"))
    country_c.place(x=235, y=120)
    country_code = ctk.CTkEntry(window, width=60, fg_color= '#e5e5e5', text_color="#000000")
    country_code.place(x=350, y=120)

    country_n = ctk.CTkLabel(window, text="Country Name", font=("Helvetica", 14, "bold"))
    country_n.place(x=235, y=160)
    country_name = ctk.CTkEntry(window, width=180, fg_color= '#e5e5e5', text_color="#000000")
    country_name.place(x=350, y=160)

    yr = ctk.CTkLabel(window, text="Year", font=("Helvetica", 14, "bold"))
    yr.place(x=235, y=200)
    year = ctk.CTkEntry(window, width=80, fg_color= '#e5e5e5', text_color="#000000")
    year.place(x=350, y=200)

    status_label = ctk.CTkLabel(window, text="", font=("Helvetica", 14, "bold"))

    del_btn = ctk.CTkButton(window, text="DELETE", font=("Helvetica", 14, "bold"), width=120, height=40, fg_color="#c1121f", hover_color="#960d17", cursor="hand2", command=delete_btn)
    del_btn.place(x=300, y=300)

def delete_btn():
    code = country_code.get()
    name = country_name.get()
    yr = year.get()
    if not (code and name and yr):
        status_label.place(x=270, y=350)
        status_label.configure(text="Please fill all information", text_color='#e63946')
    else:
        status_label.place(x=320, y=350)
        status_label.configure(text="Successful!", text_color="#6a994e")

# define buttons
add_button = ctk.CTkButton(window, text="ADD", width=140, height=50, font=("Helvetica", 16, "bold"), fg_color="#588157",hover_color="#436644", cursor="hand2", command=show_add_screen)
add_button.place(x=150, y=100)

edit_button = ctk.CTkButton(window, text="EDIT", width=140, height=50, font=("Helvetica", 16, "bold"), fg_color="#fca311", hover_color="#c2a800", cursor="hand2", command=show_edit_screen)
edit_button.place(x=400, y=100)

delete_button = ctk.CTkButton(window, text="DELETE", width=140, height=50, font=("Helvetica", 16, "bold"), fg_color="#c1121f", hover_color="#960d17", cursor="hand2", command=show_delete_screen)
delete_button.place(x=150, y=190)

view_button = ctk.CTkButton(window, text="VIEW", width=140, height=50, font=("Helvetica", 16, "bold"), fg_color="#778da9", hover_color="#415a77", cursor="hand2", command=show_view_screen)
view_button.place(x=400, y=190)

back_button = ctk.CTkButton(window, text="BACK", font=("Helvetica", 14, "bold"), width=80, height=30, fg_color="#adb5bd", hover_color="#6c757d", cursor="hand2", command=back_button_event)

window.mainloop()