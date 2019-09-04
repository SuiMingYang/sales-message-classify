import os
#图像分类
from cv2 import cv2
from PIL import Image,ImageDraw
from datetime import datetime
import time
from pytesseract import image_to_string

class Detect():
    def __init__(self):
        pass
    #detectFaces()返回图像中所有人脸的矩形坐标（矩形左上、右下顶点）
    #使用haar特征的级联分类器haarcascade_frontalface_default.xml，在haarcascades目录下还有其他的训练好的xml文件可供选择。
    #注：haarcascades目录下训练好的分类器必须以灰度图作为输入。
    def detectFaces(self,image_name):
        img = cv2.imread(image_name)
        face_cascade = cv2.CascadeClassifier("./haar/haarcascades/haarcascade_frontalface_default.xml")
        if img.ndim == 3:
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        else:
            gray = img #if语句：如果img维度为3，说明不是灰度图，先转化为灰度图gray，如果不为3，也就是2，原图就是灰度图
    
        faces = face_cascade.detectMultiScale(gray, 1.2, 5)#1.3和5是特征的最小、最大检测窗口，它改变检测结果也会改变
        result = []
        for (x,y,width,height) in faces:
            result.append((x,y,x+width,y+height))
        return result
    
    
    #保存人脸图
    def saveFaces(self,image_name):
        faces = self.detectFaces(image_name)
        if faces:
            #将人脸保存在save_dir目录下。
            #Image模块：Image.open获取图像句柄，crop剪切图像(剪切的区域就是detectFaces返回的坐标)，save保存。
            save_dir = image_name.split('.')[0]+"_faces"
            os.mkdir(save_dir)
            count = 0
            for (x1,y1,x2,y2) in faces:
                file_name = os.path.join(save_dir,str(count)+".jpg")
                Image.open(image_name).crop((x1,y1,x2,y2)).save(file_name)
                count+=1
    
    
    #在原图像上画矩形，框出所有人脸。
    #调用Image模块的draw方法，Image.open获取图像句柄，ImageDraw.Draw获取该图像的draw实例，然后调用该draw实例的rectangle方法画矩形(矩形的坐标即
    #detectFaces返回的坐标)，outline是矩形线条颜色(B,G,R)。
    #注：原始图像如果是灰度图，则去掉outline，因为灰度图没有RGB可言。drawEyes、detectSmiles也一样。
    def drawFaces(self,image_name):
        faces = self.detectFaces(image_name)
        if faces:
            img = Image.open(image_name)
            draw_instance = ImageDraw.Draw(img)
            for (x1,y1,x2,y2) in faces:
                draw_instance.rectangle((x1,y1,x2,y2), outline=(255, 0,0))
            img.save(image_name)
 
 
 

    #检测眼睛，返回坐标
    #由于眼睛在人脸上，我们往往是先检测出人脸，再细入地检测眼睛。故detectEyes可在detectFaces基础上来进行，代码中需要注意“相对坐标”。
    #当然也可以在整张图片上直接使用分类器,这种方法代码跟detectFaces一样，这里不多说。
    def detectEyes(self,image_name):
        eye_cascade = cv2.CascadeClassifier('./haar/haarcascades/haarcascade_eye.xml')
        faces = self.detectFaces(image_name)
    
        img = cv2.imread(image_name)
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        result = []
        for (x1,y1,x2,y2) in faces:
            roi_gray = gray[y1:y2, x1:x2]
            eyes = eye_cascade.detectMultiScale(roi_gray,1.3,2)
            for (ex,ey,ew,eh) in eyes:
                result.append((x1+ex,y1+ey,x1+ex+ew,y1+ey+eh))
        return result
    
    
    #在原图像上框出眼睛.
    def drawEyes(self,image_name):
        eyes = self.detectEyes(image_name)
        if eyes:
            img = Image.open(image_name)
            draw_instance = ImageDraw.Draw(img)
            for (x1,y1,x2,y2) in eyes:
                draw_instance.rectangle((x1,y1,x2,y2), outline=(0, 0,255))
            img.save(image_name)
    
 
    #检测笑脸
    def detectSmiles(self,image_name):
        img = cv2.imread(image_name)
        smiles_cascade = cv2.CascadeClassifier("./haar/haarcascades/haarcascade_smile.xml")
        if img.ndim == 3:
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        else:
            gray = img #if语句：如果img维度为3，说明不是灰度图，先转化为灰度图gray，如果不为3，也就是2，原图就是灰度图
    
        smiles = smiles_cascade.detectMultiScale(gray,4,5)
        result = []
        for (x,y,width,height) in smiles:
            result.append((x,y,x+width,y+height))
        return result
    
    
    #在原图像上框出笑脸
    def drawSmiles(self,image_name):
        smiles = self.detectSmiles(image_name)
        if smiles:
            img = Image.open(image_name)
            draw_instance = ImageDraw.Draw(img)
            for (x1,y1,x2,y2) in smiles:
                draw_instance.rectangle((x1,y1,x2,y2), outline=(100, 100,0))
            img.save(image_name)

    def img_to_str(self,url):
        image = Image.open(url)
        #识别过程
        text = image_to_string(image,lang='chi_sim')
        return text
 