# Creating shareable notebooks

To make the notebooks in this folder portable encode any images as PNG and in base64 and include as a Data URI.

Issues with other approaches,
- **Linking to images**
    Given we want users to open our demo notebooks in their preferred environment, a linked image may not be displayed, and if it is it requires us to maintain hosting of that image.
- **Embedded attachments**
    This doesn't seem to be well supported by third party viewers, e.g. in Github and Google Colab.
- **Embedded SVG files**
    Doesn't work in Github in a Markdown cell

## PNG example

It's good to keep the images small for this, modern browsers support large "Data URLs" but it can be annoying as a user users, if they open the cell it will become very large and hard to scroll, etc.

The resulting Markdown cells should contain an image tag that looks something like this small example.
```
<img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAUAAAAFCAYAAACNbyblAAAAHElEQVQI12P4//8/w38GIAXDIBKE0DHxgljNBAAO9TXL0Y4OHwAAAABJRU5ErkJggg==" alt="Red dot" />
```

### Base64 encoding

It's important to disable line wrapping with `-w0`, Google Colab doesn't handle newlines within base64 encodings, though github does. 

```
base64 -w0 images/ArcticDBLogo.svg
```
