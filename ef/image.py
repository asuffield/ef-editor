from __future__ import division
from PyQt4 import QtCore, QtGui
import Image
from ImageQt import ImageQt
import math

def make_lut(gamma, brightness, contrast):
    if gamma != 1:
        ga_fun = lambda x: x ** (1/gamma)
    else:
        ga_fun = lambda x: x
    
    # Algorithm lifted from gimp
    if brightness < 0:
        br_fun = lambda x: x * (1 + brightness)
    elif brightness > 0:
        br_fun = lambda x: x + ((1 - x) * brightness)
    else:
        br_fun = lambda x: x
    slant = math.tan((contrast + 1) * math.pi/4)
    cr_fun = lambda x: (x - 0.5) * slant + 0.5
    return lambda x: 256 * cr_fun(br_fun(ga_fun(x/256)))

class PhotoImage(QtCore.QObject):
    changed = QtCore.pyqtSignal()

    def __init__(self, image):
        QtCore.QObject.__init__(self)
        
        self.image = image
        self.rotation = self.brightness = self.contrast = 0
        self.gamma = 1
        self.crop_centre = 0.5, 0.5
        self.crop_scale = 1
        self.crop = False

    def orig_size(self):
        return self.image.size

    def set_rotation(self, degrees):
        self.rotation = degrees
        self.changed.emit()

    def set_brightness(self, v):
        self.brightness = v
        self.changed.emit()

    def set_contrast(self, v):
        self.contrast = v
        self.changed.emit()

    def set_gamma(self, v):
        self.gamma = v
        self.changed.emit()

    def set_crop(self, state):
        self.crop = state
        self.changed.emit()

    def set_crop_centre(self, x, y):
        self.crop_centre = x, y
        self.changed.emit()

    def set_crop_scale(self, scale):
        self.crop_scale = scale
        self.changed.emit()

    def make_image(self):
        image = self.image

        if self.rotation:
            image = image.rotate(self.rotation, expand=True)

        if self.crop:
            width,height = image.size
            if (width/height) > (6/8):
                crop_height = float(height)
                crop_width = crop_height * 6/8
            else:
                crop_width = float(width)
                crop_height = crop_width * 8/6
            crop_width = crop_width * self.crop_scale
            crop_height = crop_height * self.crop_scale

            x = width * self.crop_centre[0] - crop_width/2
            y = height * self.crop_centre[1] - crop_height/2
            image = image.crop((int(x), int(y), int(crop_width), int(crop_height)))

        # We don't want the alpha channel modified by the lut
        image_mask = image.split()[3]

        if self.brightness or self.contrast or self.gamma:
            image = image.point(make_lut(self.gamma, self.brightness, self.contrast))

        # This little hack creates a white background of the same
        # size, then copies all the pixels with non-zero alpha values
        # over the top of it. This sets all the transparent pixels to
        # be white instead of black.
        bg = Image.new('RGB', image.size, (255, 255, 255))
        bg.paste(image, mask=image_mask)

        return ImageQt(bg)
