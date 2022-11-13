import tkinter as tk  
from tkinter import ttk
import preprocessing 
import sqlparse
import annotation as annotation_comp

window = tk.Tk()
window.title('My Window')
window.geometry('900x600')

leftside = tk.Frame(window)
rightside = tk.Frame(window)
leftside.grid(row=0,column=0,padx = 10,pady = 5)
rightside.grid(row=0,column=1,padx = 10,pady = 5)
# create a Textbox to accept query in put
frame = tk.Frame(leftside, height=1000,width=700)
S1 = tk.Scrollbar(frame)
S1.pack(side=tk.RIGHT,fill='y')
w1 = tk.Label(window, text="query input box")
text = tk.Text(frame,height=30,width=50)
S1.config(command=text.yview)
text.configure(yscrollcommand=S1.set)
text.pack()
frame.pack()
#create atwo button
#create a abutton to submit the query
button = tk.Button(leftside, text='submit')

#one button to clear the current annoation
button1=tk.Button(leftside, text='clear')
button.pack( pady=5)
button1.pack( pady=5)

#A scrollableframe for the annoation
container = ttk.Frame(rightside,width= 700,height =700)
canvas = tk.Canvas(container,width=700,height =700)
scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
scrollable_frame = ttk.Frame(canvas,width= 700,height =700)
w1 = tk.Label(scrollable_frame, text="annoation box")
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
     raw=text.get("1.0", 'end')
     text_content = sqlparse.format(raw.strip(), strip_comments=True,
                             reindent=True, keyword_case="upper")
     preprocessing.run_preprocessing(text_content)
     anno=annotation_comp.generate_annotation(text_content)
    #  anno={0: 'anno 0', 1: 'anno 1', 2: 'anno 2', 3: 'anno 3',  4: 'anno 4'}
     
     text_array = (text_content.replace(" ","")).split("\n")
     text_array.pop()
     x=len(text_array)
     i=0
     for y in text_array:
        #generate a Textbox to show the query
        query = tk.Text(scrollable_frame,height=1,width=50)
        query.configure(font=("Courier", 12))
        query.insert(tk.INSERT,y)
        query.pack()
        #generate a Textbox for annoation
        annotation=tk.Text(scrollable_frame,height=4,width=50,background="gray", wrap=tk.WORD)
        annotation.configure(font=("Courier", 12, "italic"))
        anno_content=anno.get(i,"no annotation")
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
        text.delete("1.0",'end')
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