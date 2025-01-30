import pyperclip

clip_data = pyperclip.paste()
clip_1 = clip_data.splitlines()
clip_join_str = ', '.join(clip_1)
print(clip_join_str)
