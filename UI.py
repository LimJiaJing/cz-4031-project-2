import tkinter as tk  
from tkinter import ttk

window = tk.Tk()
window.title('My Window')
window.geometry('800x900')  
leftside = tk.Frame(window)
rightside = tk.Frame(window)
#login part of the program
login = tk.Frame(window)
#necessary infomation for login
Info = tk.Text(login,height = 4, width = 80,font=("Courier", 12, "italic"),background='gray')
Username= tk.Entry(login, width=50)
Pw = tk.Entry(login, width = 50)
Name_of_database =tk.Entry(login, width=50)
Infomation = "pls enter you SQL username, password and the name of the database\n  first line for username \n second line for username \n third line for database name"
Info.insert(tk.INSERT, Infomation)
login.pack(pady=5)
Info.pack(pady=5)
Username.pack(pady=5)
Pw.pack(pady=5)
Name_of_database.pack(pady=5)
logbutton=tk.Button(login, text='login')

# create a Textbox to accept query in put
frame = tk.Frame(window, height=1000,width=700)
S1 = tk.Scrollbar(frame)
S1.pack(side=tk.RIGHT,fill='y')
text = tk.Text(frame,height=40,width=100)
S1.config(command=text.yview)
text.configure(yscrollcommand=S1.set)
text.pack()
frame.pack()
#create atwo button
#create a abutton to submit the query
button = tk.Button(window,text='submit')

#one button to clear the current annoation
button1=tk.Button(window,text='clear')
button.pack( pady=5)
button1.pack( pady=5)

#A scrollableframe for the annoation
container = ttk.Frame(window,width= 700,height =700)
canvas = tk.Canvas(container,width=700,height =700)
scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
scrollable_frame = ttk.Frame(canvas,width= 700,height =700)
#update the canvas size to implement the scroll function
scrollable_frame.bind(
    "<Configure>",
    lambda e: canvas.configure(
        scrollregion=canvas.bbox("all")
    )
)

canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
canvas.configure(yscrollcommand=scrollbar.set)

logbutton.pack(pady=5)

# Function for submit buton
def get():
     anno={0: 'anno 0', 1: 'anno 1', 2: 'anno 2', 3: 'anno 3',  4: 'anno 4'}
     text_content = (text.get("0.0","end").replace(" ","")).split("\n")
     text_content.pop()
     x=len(text_content)
     i=0
     for y in text_content:
        #generate a Textbox to show the query
        query = tk.Text(scrollable_frame,height=1,width=100)
        query.configure(font=("Courier", 12))
        query.insert(tk.INSERT,y)
        query.pack()
        #generate a Textbox for annoation
        annotation=tk.Text(scrollable_frame,height=2,width=100,background="gray" )
        annotation.configure(font=("Courier", 12, "italic"))
        anno_content=anno.get(i,"no anno")
        annotation.insert(tk.INSERT, anno_content)
        annotation.pack()
        #fix the Textbox after generation
        query.config(state='disabled')
        annotation.config(state= 'disable')
        i=i+1
        button["state"]="disable"
#Clear the annoation frame
def clear():
    for query in scrollable_frame.winfo_children():
        query.destroy()
    button["state"]="normal"

def loginfunction():
   User = Username.get()
   if User == '123':
     button.config(command=get)
     button1.config(command=clear)
     print(User)
   else:
     newWindow = tk.Toplevel(window)
     newWindow.geometry("360x200")
     labelExample = tk.Label(newWindow, text = "failed to login", font=("Courier", 12, "italic"))

     labelExample.place(x=90, y=70)
logbutton.config(command=loginfunction)
container.pack()
canvas.pack(side="left", fill="both", expand=True)
scrollbar.pack(side="right", fill="y")

#continuing refresh the window
window.mainloop()