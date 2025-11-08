# pip install customtkinter
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
ctk.set_appearance_mode("dark")
window.configure(fg_color="#000000")


def hide_all_buttons():
    add_button.place_forget()
    edit_button.place_forget()
    delete_button.place_forget()
    view_button.place_forget()
    back_button.place(x=600, y=15)

# clear everything in the window except background
def clear_screen(exceptions=()):
    for w in window.winfo_children():
        if w in exceptions:
            continue
        w.place_forget()

# defind button event
def back_button_event():
    clear_screen(exceptions=[bg_label])
    add_button.place(x=150, y=100)
    edit_button.place(x=400, y=100)
    delete_button.place(x=150, y=190)
    view_button.place(x=400, y=190)
    back_button.place_forget()

def show_add_screen():
    hide_all_buttons()

    country_c = ctk.CTkLabel(window, text="Country Code", font=("Helvetica", 12, "bold"))
    country_c.place(x=235, y=80)
    country_c2 = ctk.CTkEntry(window, width=60, fg_color= '#e5e5e5')
    country_c2.place(x=350, y=80)

    country_n = ctk.CTkLabel(window, text="Country Name", font=("Helvetica", 12, "bold"))
    country_n.place(x=235, y=120)
    country_n2 = ctk.CTkEntry(window, width=180, fg_color= '#e5e5e5')
    country_n2.place(x=350, y=120)

    n_people = ctk.CTkLabel(window, text="Number of People", font=("Helvetica", 12, "bold"))
    n_people.place(x=235, y=160)
    n_people2 = ctk.CTkEntry(window, width=120, fg_color= '#e5e5e5')
    n_people2.place(x=350, y=160)

    year = ctk.CTkLabel(window, text="Year", font=("Helvetica", 12, "bold"))
    year.place(x=235, y=200)
    year2 = ctk.CTkEntry(window, width=80, fg_color= '#e5e5e5')
    year2.place(x=350, y=200)

    add_confirm = ctk.CTkButton(window, text="ADD", width=120, height=40, fg_color="#588157", hover_color="#436644", cursor="hand2")
    add_confirm.place(x=300, y=300)

def show_edit_screen():
    hide_all_buttons()

    country_c = ctk.CTkLabel(window, text="Country Code", font=("Helvetica", 12, "bold"))
    country_c.place(x=235, y=80)
    country_c2 = ctk.CTkEntry(window, width=60, fg_color= '#e5e5e5')
    country_c2.place(x=350, y=80)

    country_n = ctk.CTkLabel(window, text="Country Name", font=("Helvetica", 12, "bold"))
    country_n.place(x=235, y=120)
    country_n2 = ctk.CTkEntry(window, width=180, fg_color= '#e5e5e5')
    country_n2.place(x=350, y=120)

    n_people = ctk.CTkLabel(window, text="Number of People", font=("Helvetica", 12, "bold"))
    n_people.place(x=235, y=160)
    n_people2 = ctk.CTkEntry(window, width=120, fg_color= '#e5e5e5')
    n_people2.place(x=350, y=160)

    year = ctk.CTkLabel(window, text="Year", font=("Helvetica", 12, "bold"))
    year.place(x=235, y=200)
    year2 = ctk.CTkEntry(window, width=80, fg_color= '#e5e5e5')
    year2.place(x=350, y=200)

    save_btn = ctk.CTkButton(window, text="SAVE", width=120, height=40, fg_color="#0077b6", hover_color="#025b87", cursor="hand2")
    save_btn.place(x=300, y=300)

def show_delete_screen():
    hide_all_buttons()

    country_c = ctk.CTkLabel(window, text="Country Code", font=("Helvetica", 12, "bold"))
    country_c.place(x=235, y=120)
    country_c2 = ctk.CTkEntry(window, width=60, fg_color= '#e5e5e5')
    country_c2.place(x=350, y=120)

    country_n = ctk.CTkLabel(window, text="Country Name", font=("Helvetica", 12, "bold"))
    country_n.place(x=235, y=160)
    country_n2 = ctk.CTkEntry(window, width=180, fg_color= '#e5e5e5')
    country_n2.place(x=350, y=160)

    year = ctk.CTkLabel(window, text="Year", font=("Helvetica", 12, "bold"))
    year.place(x=235, y=200)
    year2 = ctk.CTkEntry(window, width=80, fg_color= '#e5e5e5')
    year2.place(x=350, y=200)

    del_btn = ctk.CTkButton(window, text="DELETE", width=120, height=40, fg_color="#c1121f", hover_color="#960d17", cursor="hand2")
    del_btn.place(x=300, y=300)


def show_view_screen():
    hide_all_buttons()

    country_c = ctk.CTkLabel(window, text="Country Code", font=("Helvetica", 12, "bold"))
    country_c.place(x=215, y=120)
    country_c2 = ctk.CTkEntry(window, width=60, fg_color= '#e5e5e5')
    country_c2.place(x=320, y=120)

    label = ctk.CTkLabel(window, text="OR", font=("Helvetica", 12, "bold"))
    label.place(x=330, y=160)

    country_n = ctk.CTkLabel(window, text="Country Name", font=("Helvetica", 12, "bold"))
    country_n.place(x=215, y=200)
    country_n2 = ctk.CTkEntry(window, width=180, fg_color= '#e5e5e5')
    country_n2.place(x=320, y=200)

    search_btn = ctk.CTkButton(window, text="SEARCH", width=120, height=40, fg_color="#4a4e69", hover_color="#34344d", cursor="hand2")
    search_btn.place(x=300, y=300)


# define buttons
add_button = ctk.CTkButton(window, text="ADD", width=140, height=50, font=("Helvetica", 16, "bold"), fg_color="#588157",hover_color="#436644", cursor="hand2", command=show_add_screen)
add_button.place(x=150, y=100)

edit_button = ctk.CTkButton(window, text="EDIT", width=140, height=50, font=("Helvetica", 16, "bold"), fg_color="#fca311", hover_color="#c2a800", cursor="hand2", command=show_edit_screen)
edit_button.place(x=400, y=100)

delete_button = ctk.CTkButton(window, text="DELETE", width=140, height=50, font=("Helvetica", 16, "bold"), fg_color="#c1121f", hover_color="#960d17", cursor="hand2", command=show_delete_screen)
delete_button.place(x=150, y=190)

view_button = ctk.CTkButton(window, text="VIEW", width=140, height=50, font=("Helvetica", 16, "bold"), fg_color="#4a4e69", hover_color="#34344d", cursor="hand2", command=show_view_screen)
view_button.place(x=400, y=190)

back_button = ctk.CTkButton(window, text="BACK", width=80, height=30, fg_color="#adb5bd", hover_color="#6c757d", cursor="hand2", command=back_button_event)

window.mainloop()