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
    
    # Add query
    label = tk.Label(window, text="Country Code", font=("Helvetica", 9, "bold"))
    label.place(x=235, y=80)
    entry_var = tk.StringVar()
    entry = tk.Entry(window, textvariable=entry_var, width=5, font=("Helvetica", 9))
    entry.place(x=350, y=80)

    label = tk.Label(window, text="Country Name", font=("Helvetica", 9, "bold"))
    label.place(x=235, y=120)
    entry_var = tk.StringVar()
    entry = tk.Entry(window, textvariable=entry_var, width=20, font=("Helvetica", 9))
    entry.place(x=350, y=120)

    label = tk.Label(window, text="Number of people", font=("Helvetica", 9, "bold"))
    label.place(x=235, y=160)
    entry_var = tk.StringVar()
    entry = tk.Entry(window, textvariable=entry_var, width=12, font=("Helvetica", 9))
    entry.place(x=350, y=160)

    label = tk.Label(window, text="Year", font=("Helvetica", 9, "bold"))
    label.place(x=235, y=200)
    entry_var = tk.StringVar()
    entry = tk.Entry(window, textvariable=entry_var, width=6, font=("Helvetica", 9))
    entry.place(x=350, y=200)

    # Execute
    confirm_button = tk.Button(window, text="ADD", font=("Helvetica", 14, "bold"), width=8, bg="#588157", fg="white", cursor='hand2')
    confirm_button.place(x=300, y=300)

def show_edit_screen():
    hide_all_buttons()

    label = tk.Label(window, text="Country Code", font=("Helvetica", 9, "bold"))
    label.place(x=235, y=80)
    entry_var = tk.StringVar()
    entry = tk.Entry(window, textvariable=entry_var, width=5, font=("Helvetica", 9))
    entry.place(x=350, y=80)

    label = tk.Label(window, text="Country Name", font=("Helvetica", 9, "bold"))
    label.place(x=235, y=120)
    entry_var = tk.StringVar()
    entry = tk.Entry(window, textvariable=entry_var, width=20, font=("Helvetica", 9))
    entry.place(x=350, y=120)

    label = tk.Label(window, text="Number of people", font=("Helvetica", 9, "bold"))
    label.place(x=235, y=160)
    entry_var = tk.StringVar()
    entry = tk.Entry(window, textvariable=entry_var, width=12, font=("Helvetica", 9))
    entry.place(x=350, y=160)

    label = tk.Label(window, text="Year", font=("Helvetica", 9, "bold"))
    label.place(x=235, y=200)
    entry_var = tk.StringVar()
    entry = tk.Entry(window, textvariable=entry_var, width=6, font=("Helvetica", 9))
    entry.place(x=350, y=200)

    save_button = tk.Button(window, text="SAVE", font=("Helvetica", 14, "bold"), bg="#00b4d8", fg="white", cursor='hand2')
    save_button.place(x=300, y=300)

def show_delete_screen():
    hide_all_buttons()

    label = tk.Label(window, text="Country Code", font=("Helvetica", 9, "bold"))
    label.place(x=235, y=120)
    entry_var = tk.StringVar()
    entry = tk.Entry(window, textvariable=entry_var, width=5, font=("Helvetica", 9))
    entry.place(x=350, y=120)

    label = tk.Label(window, text="Country Name", font=("Helvetica", 9, "bold"))
    label.place(x=235, y=160)
    entry_var = tk.StringVar()
    entry = tk.Entry(window, textvariable=entry_var, width=20, font=("Helvetica", 9))
    entry.place(x=350, y=160)

    label = tk.Label(window, text="Year", font=("Helvetica", 9, "bold"))
    label.place(x=235, y=200)
    entry_var = tk.StringVar()
    entry = tk.Entry(window, textvariable=entry_var, width=6, font=("Helvetica", 9))
    entry.place(x=350, y=200)

    del_button = tk.Button(window, text="DELETE", font=("Helvetica", 14, "bold"), bg="#c1121f", fg="white", cursor='hand2')
    del_button.place(x=300, y=300)

def show_view_screen():
    hide_all_buttons()

def back_button_event():
    for i in window.winfo_children():
        if i is bg_label:
            continue
        i.place_forget()
    add_button.place(x=150, y=100)
    edit_button.place(x=400, y=100)
    delete_button.place(x=150, y=190)
    view_button.place(x=400, y=190)
    
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

