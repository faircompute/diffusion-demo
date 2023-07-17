import gradio as gr
from fair import text_to_image


def run(text):
    image_path = text_to_image(text)
    print(image_path)
    return image_path

def main():
    demo = gr.Interface(
        fn=run,
        inputs=gr.Textbox(lines=2, placeholder="Robot dinosaur"),
        outputs=gr.Image(type="filepath")
    )
    demo.launch()

if __name__ == "__main__":
    main()
