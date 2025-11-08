import tkinter as tk
import tkinter.font as tkFont


# Set up window size, position
window = tk.Tk()
window.title("Electricity Database Management")
window_width = 700
window_height = 400
screen_width = window.winfo_screenwidth()
screen_height = window.winfo_screenheight()
x = int((screen_width / 2) - (window_width / 2))
y = int((screen_height / 2) - (window_height / 2))
window.geometry(f'{window_width}x{window_height}+{x}+{y}')

# Window icon
window.iconbitmap("icon.ico")

# Set backround image
bg_img = tk.PhotoImage(file="background.png")
bg_label = tk.Label(window, image=bg_img)
bg_label.place(x=0, y=0, relwidth=1, relheight=1)

# Font size
font_large = tkFont.Font(family="Helvetica", size=16, weight="bold")

def hide_all_buttons():
    add_button.place_forget()
    edit_button.place_forget()
    delete_button.place_forget()
    view_button.place_forget()
    back_button.place(x=600, y=20)

def show_add_screen():
    hide_all_buttons()
    add_content = True
    # Create query
    if add_content:
        label = tk.Label(window, text="Enter SQL Query:", font=("Helvetica", 14, "bold"))
        label.place(x=275, y=50)

        entry_var = tk.StringVar()
        entry = tk.Entry(window, textvariable=entry_var, width=50, font=("Helvetica", 12))
        entry.place(x=50, y=140)

    # Execute
    confirm_button = tk.Button(window, text="ADD", font=("Helvetica", 14, "bold"), bg="#588157", fg="white")
    confirm_button.place(x=300, y=200)

def show_edit_screen():
    hide_all_buttons()

def show_delete_screen():
    hide_all_buttons()

def show_view_screen():
    hide_all_buttons()

def back_button_event():
    add_button.place(x=150, y=100)
    edit_button.place(x=400, y=100)
    delete_button.place(x=150, y=190)
    view_button.place(x=400, y=190)
    back_button.place_forget()

#  Define buttons
add_button = tk.Button(window, text="ADD", width=10, height=2, font=font_large, command=show_add_screen, bg="#588157", cursor='hand2')
add_button.place(x=150, y=100)
add_content = False
edit_button = tk.Button(window, text="EDIT", command=show_edit_screen, width=10, height=2, font=font_large, bg="#ffd60a", cursor='hand2')
edit_button.place(x=400, y=100)
delete_button = tk.Button(window, text="DELETE", command=show_delete_screen, width=10, height=2, font=font_large, bg="#c1121f", cursor='hand2')
delete_button.place(x=150, y=190)
view_button = tk.Button(window, text="VIEW", command=show_view_screen, width=10, height=2, font=font_large, cursor='hand2')
view_button.place(x=400, y=190)
back_button = tk.Button(window, text="BACK", command=back_button_event, font=font_large, cursor='hand2')

window.mainloop()

