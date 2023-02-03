import os
import zmq
import tango
import numpy as np
from threading import Thread
from tango import DevState
from tango.server import Device, attribute, command, run, device_property
import signal
import os
#from . import andor
#from . import atutility

import andor 
import atutility

class Andor3(Device):

    def __init__(self, *args, **kwargs):
        self.context = zmq.Context()
        self.pipe = self.context.socket(zmq.PAIR)
        self.pipe.bind('inproc://zyla')
        self.data_socket = self.context.socket(zmq.PUSH)
        self.data_socket.bind(os.environ.get("DATA_SOCKET", 'tcp://*:9999'))
        self._msg_number = 0

        # this internally calls init_device
        super().__init__(*args, **kwargs)

        self.thread = Thread(target=self.main)
        self.thread.start()


    def init_device(self):
        super().init_device()
        andor.sdk.AT_InitialiseLibrary()
        devcount = andor.get_int(andor.AT_HANDLE_SYSTEM, 'DeviceCount')
        handle = andor.ffi.new('AT_H*')
        andor.sdk.AT_Open(0, handle)
        self.handle = handle[0]
        print('Found', devcount, ' devices')
        self._camera_model = andor.get_string(self.handle, 'CameraModel')
        print('CameraModel', self._camera_model)

        self._filename = ''
        self._frame_count = 1
        self._acquired_frames = 0
        # -1 is error, 0 is idle and 1 is running
        self._running = 0
        self._fliplr = False
        self._flipud = False
        self._rotation = 0
        
        self._exposure_time = andor.get_float(self.handle, 'ExposureTime')
        self._trigger_mode = andor.get_enum_string(self.handle, 'TriggerMode')
        self._shutter_mode = andor.get_enum_string(self.handle, 'ElectronicShutteringMode')
        self._pixel_readout_rate = andor.get_enum_string(self.handle, 'PixelReadoutRate')
        self._sensor_cooling = andor.get_bool(self.handle, 'SensorCooling')
        self._hbin = andor.get_int(self.handle, 'AOIHBin')
        self._width = andor.get_int(self.handle, 'AOIWidth')
        self._left = andor.get_int(self.handle, 'AOILeft')
        self._vbin = andor.get_int(self.handle, 'AOIVBin')
        self._height = andor.get_int(self.handle, 'AOIHeight')
        self._top = andor.get_int(self.handle, 'AOITop')
        
        atutility.sdk.AT_InitialiseUtilityLibrary()
        
        #print(andor.get_enum_string_options(self.handle, 'TemperatureControl'))
        
        options = andor.get_enum_string_options(self.handle, 'SimplePreAmpGainControl')
        self._gain_control_options = '\n'.join(options)
        
        andor.set_enum_string(self.handle, 'CycleMode', 'Fixed')
        
        self.buffers = []

        self.register_signal(signal.SIGINT)
        self.set_state(DevState.ON)
    
    def delete_device(self):
        print('delete_device')
        self.context.destroy()

    def signal_handler(self, signo):
        self.pipe.send(b'terminate')
        self.thread.join(1)

    def update_state_and_status(self):
        if self._running == 0:
            state, status = DevState.ON, 'Idle'
        elif self._running == 1:
            state, status = DevState.RUNNING, 'Acquisition in progress'
        else:
            state, status = DevState.ERROR, 'Error'
        self.set_state(state)
        self.set_status(status)
            
    def dev_state(self):
        self.update_state_and_status()
        return self.get_state()
    
    def dev_status(self):
        self.update_state_and_status()
        return self.get_status()
        
    def queue_buffer(self, buf, size):
        andor.sdk.AT_QueueBuffer(self.handle, buf, size)
        
    def handle_image(self, buf, size):
        img = np.empty((self._height, self._width), dtype=np.uint16)
        ret = atutility.sdk.AT_ConvertBuffer(buf, 
                                             andor.ffi.from_buffer(img),
                                             self._width, self._height,
                                             self.stride, self.pixel_encoding,
                                             'Mono16')
        if ret != 0:
            raise RuntimeError('Error in AT_ConvertBuffer')
        # return buffer to andor sdk
        self.queue_buffer(buf, size)
                
        if self._fliplr:
            img = np.fliplr(img)
                    
        if self._flipud:
            img = np.flipud(img)
                
        if self._rotation:
            img = np.rot90(img, self._rotation)
            
        img = np.ascontiguousarray(img)
        return img
    
    def main(self):
        pipe = self.context.socket(zmq.PAIR)
        pipe.connect('inproc://zyla')
        fd_video = os.open('/dev/video0', os.O_RDONLY)
        poller = zmq.Poller()
        poller.register(fd_video, zmq.POLLIN)
        poller.register(pipe, zmq.POLLIN)

        def finish():
            if self._running:
                self.data_socket.send_json({'htype': 'series_end',
                                            'msg_number': self._msg_number})
                self._msg_number += 1
            andor.sdk.AT_Command(self.handle, 'AcquisitionStop')
            andor.sdk.AT_Flush(self.handle)
            self._running = 0
        
        while True:
            events = dict(poller.poll())
            if fd_video in events and events[fd_video] == zmq.POLLIN:
                ret = andor.wait_buffer(self.handle, 0)
                if ret is None:
                    continue
                buf, size = ret
                #print('frame', self._acquired_frames)
                img = self.handle_image(buf, size)
                frame = zmq.Frame(img, copy=False)
                self.data_socket.send_json({'htype': 'image',
                                  'frame': self._acquired_frames,
                                  'shape': img.shape,
                                  'type': 'uint16',
                                  'compression': 'none',
                                  'msg_number': self._msg_number}, flags=zmq.SNDMORE)
                self.data_socket.send(frame, copy=False)
                self._msg_number += 1
                self._acquired_frames += 1
                if self._acquired_frames == self._frame_count:
                    finish()
                
            if pipe in events and events[pipe] == zmq.POLLIN:
                msg = pipe.recv()
                if msg == b'start':
                    print('start')
                    self._running = 1
                    self.data_socket.send_json({'htype': 'header',
                                                'filename': self._filename,
                                                'msg_number': self._msg_number}, flags=zmq.SNDMORE)
                    self.data_socket.send_json({"cooling": self._sensor_cooling,
                                                "hbin": self._hbin,
                                                "vbin": self._vbin,
                                                })
                    self._msg_number += 1
                elif msg == b'stop':
                    print('end')
                    finish()

                elif msg == b'terminate':
                    print('terminating network thread')
                    finish()
                    break
            
            
    @command
    def Arm(self):
        print('start', self._frame_count)
        self.stride = andor.get_int(self.handle, 'AOIStride')
        self.pixel_encoding = andor.get_enum_string(self.handle, 'PixelEncoding')
        print(self._height, self._width, self.stride, self.pixel_encoding)
        print('ReadoutTime', andor.get_float(self.handle, 'ReadoutTime'))
        print('ImageSizeBytes', andor.get_int(self.handle, 'ImageSizeBytes'))
        image_size = andor.get_int(self.handle, 'ImageSizeBytes')
        self._acquired_frames = 0
        for i in range(100):
            buf = np.empty(image_size, np.uint8)
            self.buffers.append(buf)
        andor.sdk.AT_Flush(self.handle)
        for buf in self.buffers:
            andor.sdk.AT_QueueBuffer(self.handle, andor.ffi.from_buffer(buf), image_size)
        self.pipe.send(b'start')
        andor.sdk.AT_Command(self.handle, 'AcquisitionStart')
        
    @command
    def SoftwareTrigger(self):
        andor.sdk.AT_Command(self.handle, 'SoftwareTrigger')
        
    @command
    def Stop(self):
        print('stop')
        self.pipe.send(b'stop')
        
    @attribute(dtype=str)
    def CameraModel(self):
        return self._camera_model

    @attribute(dtype=int)
    def nFramesAcquired(self):
        return self._acquired_frames
        
    @attribute(dtype=str)
    def Filename(self):
        return self._filename
    
    @Filename.setter
    def Filename(self, value):
        self._filename = value
        
    @attribute(dtype=int)
    def nTriggers(self):
        return self._frame_count
    
    @nTriggers.setter
    def nTriggers(self, value):
        andor.sdk.AT_SetInt(self.handle, 'FrameCount', value)
        self._frame_count = value
        
    @attribute(dtype=float)
    def ExposureTime(self):
        return self._exposure_time
    
    @ExposureTime.setter
    def ExposureTime(self, value):
        ret = andor.sdk.AT_SetFloat(self.handle, 'ExposureTime', value)
        if ret != 0:
            raise RuntimeError('Error setting exposure time: %s' %andor.errors.get(ret, ''))
        self._exposure_time = andor.get_float(self.handle, 'ExposureTime')
        
    @attribute(dtype=bool)
    def Overlap(self):
        ret = andor.get_bool(self.handle, 'Overlap')
        value = True if ret == 1 else False
        return value
    
    @Overlap.setter
    def Overlap(self, value):
        attr = 1 if value == True else 0
        andor.sdk.AT_SetBool(self.handle, 'Overlap', attr)
    
    @attribute(dtype=str)
    def SimplePreAmpGainControl(self):
        ret = andor.get_enum_string(self.handle, 'SimplePreAmpGainControl')
        return ret
    
    @SimplePreAmpGainControl.setter
    def SimplePreAmpGainControl(self, value):
        andor.set_enum_string(self.handle, 'SimplePreAmpGainControl', value)
        if "12-bit" in value:
            andor.set_enum_string(self.handle, 'PixelEncoding', "Mono12Packed")


    @attribute(dtype=str)
    def SimplePreAmpGainControlOptions(self):
        return self._gain_control_options
        
    @attribute(dtype=str)
    def TriggerMode(self):
        return self._trigger_mode
    
    @TriggerMode.setter
    def TriggerMode(self, value):
        andor.set_enum_string(self.handle, 'TriggerMode', value)
        self._trigger_mode = value
       
    @attribute(dtype=float)
    def FrameRate(self):
        return andor.get_float(self.handle, 'FrameRate')
    
    @FrameRate.setter
    def FrameRate(self, value):
        ret = andor.sdk.AT_SetFloat(self.handle, 'FrameRate', value)
        if ret != 0:
            raise RuntimeError('Error setting FrameRate: %s' %andor.errors.get(ret, ''))
        #self._frame_rate = andor.get_float(self.handle, 'FrameRate')
        
    @attribute(dtype=str)
    def ElectronicShutteringMode(self):
        return self._shutter_mode
        
    @ElectronicShutteringMode.setter
    def ElectronicShutteringMode(self, value):
        andor.set_enum_string(self.handle, 'ElectronicShutteringMode', value)
        self._shutter_mode = value
        
    @attribute(dtype=str)
    def PixelReadoutRate(self):
        return self._pixel_readout_rate
    
    @PixelReadoutRate.setter
    def PixelReadoutRate(self, value):
        andor.set_enum_string(self.handle, 'PixelReadoutRate', value)
        self._pixel_readout_rate = value
    
    @attribute(dtype=str)
    def PixelEncoding(self):
        return andor.get_enum_string(self.handle, 'PixelEncoding')

    @attribute(dtype=float)
    def SensorTemperature(self):
        return andor.get_float(self.handle, 'SensorTemperature')
    
    
    # ROI attributes
    
    @attribute(dtype=int)
    def AOIHBin(self):
        return self._hbin
    
    @AOIHBin.setter
    def AOIHBin(self, value):
        andor.set_int(self.handle, 'AOIHBin', value)
        self._hbin = value
        self._width = andor.get_int(self.handle, 'AOIWidth')
        
    @attribute(dtype=int)
    def AOIWidth(self):
        return self._width
    
    @AOIWidth.setter
    def AOIWidth(self, value):
        andor.set_int(self.handle, 'AOIWidth', value)
        self._width = value
    
    @attribute(dtype=int)
    def AOILeft(self):
        return self._left
    
    @AOILeft.setter
    def AOILeft(self, value):
        andor.set_int(self.handle, 'AOILeft', value)
        self._left = value
    
    @attribute(dtype=int)
    def AOIVBin(self):
        return self._vbin
    
    @AOIVBin.setter
    def AOIVBin(self, value):
        andor.set_int(self.handle, 'AOIVBin', value)
        self._vbin = value
        self._height = andor.get_int(self.handle, 'AOIHeight')
        
    @attribute(dtype=int)
    def AOIHeight(self):
        return self._height
    
    @AOIHeight.setter
    def AOIHeight(self, value):
        andor.set_int(self.handle, 'AOIHeight', value)
        self._height = value
    
    @attribute(dtype=int)
    def AOITop(self):
        return self._top
    
    @AOITop.setter
    def AOITop(self, value):
        andor.set_int(self.handle, 'AOITop', value)
        self._top = value
    
    @attribute(dtype=bool)
    def SensorCooling(self):
        return self._sensor_cooling
    
    @SensorCooling.setter
    def SensorCooling(self, value):
        attr = 1 if value == True else 0
        andor.sdk.AT_SetBool(self.handle, 'SensorCooling', attr)
        self._sensor_cooling = value
        
    @attribute(dtype=str)
    def TemperatureStatus(self):
        return andor.get_enum_string(self.handle, 'TemperatureStatus')
        
    @attribute(dtype=bool, memorized=True, hw_memorized=True)
    def Fliplr(self):
        return self._fliplr
    
    @Fliplr.setter
    def Fliplr(self, value):
        self._fliplr = value
        
    @attribute(dtype=bool, memorized=True, hw_memorized=True)
    def Flipud(self):
        return self._flipud
    
    @Flipud.setter
    def Flipud(self, value):
        self._flipud = value
        
    @attribute(dtype=int, memorized=True, hw_memorized=True)
    def Rotation(self):
        return self._rotation
    
    @Rotation.setter
    def Rotation(self, value):
        self._rotation = value
        
def main():
    
    dev_info = tango.DbDevInfo()
    dev_info._class = 'Andor3'
    dev_info.server = 'Andor3/test'
    dev_info.name = 'zyla/test/1'

    db = tango.Database()
    db.add_device(dev_info)
    
    Andor3.run_server()
    
main()
