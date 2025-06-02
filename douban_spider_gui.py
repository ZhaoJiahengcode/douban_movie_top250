import tkinter as tk
from tkinter import messagebox
import threading

# 导入你的爬虫主函数
from run import main as run_crawler

class App:
    def __init__(self, master):
        self.master = master
        master.title("豆瓣Top250爬虫")
        master.geometry("300x200")

        self.label = tk.Label(master, text="豆瓣Top250电影爬虫", font=(None, 14))
        self.label.pack(pady=10)

        self.start_btn = tk.Button(master, text="开始爬取", width=20, command=self.start_crawl)
        self.start_btn.pack(pady=5)

        self.open_btn = tk.Button(master, text="打开结果文件", width=20, command=self.open_file)
        self.open_btn.pack(pady=5)

        self.status = tk.Label(master, text="状态：等待操作", fg="blue")
        self.status.pack(pady=10)

    def start_crawl(self):
        self.start_btn.config(state=tk.DISABLED)
        self.status.config(text="状态：正在爬取...")
        threading.Thread(target=self.run_task).start()

    def run_task(self):
        try:
            run_crawler()
            self.status.config(text="状态：爬取完成！")
            messagebox.showinfo("完成", "豆瓣Top250电影爬取并分析完成！")
        except Exception as e:
            messagebox.showerror("错误", f"爬取失败：{e}")
            self.status.config(text="状态：爬取失败")
        finally:
            self.start_btn.config(state=tk.NORMAL)

    def open_file(self):
        import os
        filepath = os.path.abspath("output/movies.xlsx")
        if os.path.exists(filepath):
            os.startfile(filepath)
        else:
            messagebox.showwarning("警告", "结果文件不存在！")

if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
