import gradio as gr

def run(text):
    pass

def main():
    demo = gr.Interface(
        fn=run,
        inputs=gr.Textbox(lines=2, placeholder="Robot dinosaur"),
        outputs="image"
    )
    demo.launch()
