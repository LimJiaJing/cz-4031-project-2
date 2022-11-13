import tkinter as tk  
from tkinter import ttk

window = tk.Tk()
window.title('My Window')
window.geometry('800x900')

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
button = tk.Button(window, text='submit')

#one button to clear the current annoation
button1=tk.Button(window, text='clear')
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



# Function for submit buton
def get():
     anno={0: 'anno 0', 1: 'anno 1', 2: 'anno 2', 3: 'anno 3',  4: 'anno 4'}
     text_content = sqlparse.format(raw.strip(), strip_comments=True,
                             reindent=True, keyword_case="upper")
     text_content = (text_content.replace(" ","")).split("\n")
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

def queryget():
    query_content = text.get()
    return query_content

container.pack()
canvas.pack(side="left", fill="both", expand=True)
scrollbar.pack(side="right", fill="y")
button.config(command=get)
button1.config(command=clear)
#continuing refresh the window
window.mainloop()