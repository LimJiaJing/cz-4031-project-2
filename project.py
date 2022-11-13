import preprocessing
import annotation
import UI
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
container.pack()
canvas.pack(side="left", fill="both", expand=True)
scrollbar.pack(side="right", fill="y")
button.config(command=get)
button1.config(command=clear)
#continuing refresh the window
window.mainloop()


preprocessing.run_preprocessing()
annotation.generate_annotation(sql)
