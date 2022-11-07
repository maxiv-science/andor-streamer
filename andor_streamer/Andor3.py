import os
import zmq
import tango
import numpy as np
from threading import Thread
from tango import DevState
from tango.server import Device, attribute, command, run, device_property
from . import andor 
from . import atutility

class Andor3(Device):
    def __init__(self, cl, name):
        Device.__init__(self, cl, name)
        andor.sdk.AT_InitialiseLibrary()
        devcount = andor.get_int(andor.AT_HANDLE_SYSTEM, 'DeviceCount')
        handle = andor.ffi.new('AT_H*')
        andor.sdk.AT_Open(0, handle)
        self.handle = handle[0]
        print('Found', devcount, ' devices')
        print('CameraModel', andor.get_string(self.handle, 'CameraModel'))
        
        self.context = zmq.Context()
        self.pipe = self.context.socket(zmq.PAIR)
        self.pipe.bind('inproc://zyla')
        self.data_socket = self.context.socket(zmq.PUSH)
        self.data_socket.bind('tcp://*:9999')
        self.monitor_socket = self.context.socket(zmq.REP)
        self.monitor_socket.bind('tcp://*:9998')
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
        self._frame_rate = andor.get_float(self.handle, 'FrameRate')
        self._sensor_cooling = andor.get_bool(self.handle, 'SensorCooling')
        
        atutility.sdk.AT_InitialiseUtilityLibrary()
        andor.set_enum_string(self.handle, 'SimplePreAmpGainControl', '16-bit (low noise & high well capacity)')
        andor.set_enum_string(self.handle, 'TriggerMode', 'Internal')
        andor.set_enum_string(self.handle, 'CycleMode', 'Fixed')
        
        image_size = andor.get_int(self.handle, 'ImageSizeBytes')
        print('ImageSizeBytes', image_size)
        self.buffers = []
        for i in range(100):
            buf = np.empty(image_size, np.uint8)
            self.buffers.append(buf)
            
        self.thread = Thread(target=self.main)
        self.thread.start()
        
    def init_device(self):
        self.set_state(DevState.ON)
        
    def always_executed_hook(self):
        #print('get state')
        if self._running == 0:
            self.set_state(DevState.ON)
        elif self._running == 1:
            self.set_state(DevState.RUNNING)
        else:
            self.set_state(DevState.ERROR)
        
    def queue_buffer(self, buf, size):
        andor.sdk.AT_QueueBuffer(self.handle, buf, size)
        
    def handle_image(self, buf, size):
        img = np.empty((self.height, self.width), dtype=np.uint16)
        ret = atutility.sdk.AT_ConvertBuffer(buf, 
                                             andor.ffi.from_buffer(img),
                                             self.width, self.height,
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
        poller.register(self.monitor_socket, zmq.POLLIN)
        last_frame = zmq.Frame()
        
        def finish():
            if self._running:
                self.data_socket.send_json({'htype': 'series_end'})
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
                last_frame = frame
                self.data_socket.send_json({'htype': 'image',
                                  'frame': self._acquired_frames,
                                  'shape': img.shape,
                                  'type': 'uint16',
                                  'compression': 'none'}, flags=zmq.SNDMORE)
                self.data_socket.send(frame, copy=False)
                self._acquired_frames += 1
                if self._acquired_frames == self._frame_count:
                    finish()
                
            if pipe in events and events[pipe] == zmq.POLLIN:
                msg = pipe.recv()
                if msg == b'start':
                    print('start')
                    self._running = 1
                    self.data_socket.send_json({'htype': 'header',
                                                'filename': self._filename})
                elif msg == b'stop':
                    print('end')
                    finish()
                    
            if self.monitor_socket in events and events[self.monitor_socket] == zmq.POLLIN:
                print(self.monitor_socket.recv())
                self.monitor_socket.send_json({'htype': 'image',
                                  'frame': self._acquired_frames,
                                  'shape': [self.height, self.width],
                                  'type': 'uint16',
                                  'compression': 'none'}, flags=zmq.SNDMORE)
                self.monitor_socket.send(last_frame, copy=False)
                print('send monitoring frame')
            
            
    @command
    def Arm(self):
        print('start', self._frame_count)
        self.height = andor.get_int(self.handle, 'AOIHeight')
        self.width = andor.get_int(self.handle, 'AOIWidth')
        self.stride = andor.get_int(self.handle, 'AOIStride')
        self.pixel_encoding = andor.get_enum_string(self.handle, 'PixelEncoding')
        print(self.height, self.width, self.stride, self.pixel_encoding)
        print('ReadoutTime', andor.get_float(self.handle, 'ReadoutTime'))
        self._acquired_frames = 0
        self.pipe.send(b'start')
        for buf in self.buffers:
            andor.sdk.AT_QueueBuffer(self.handle, andor.ffi.from_buffer(buf), buf.nbytes)
        andor.sdk.AT_Command(self.handle, 'AcquisitionStart')
        
    @command
    def SoftwareTrigger(self):
        andor.sdk.AT_Command(self.handle, 'SoftwareTrigger')
        
    @command
    def Stop(self):
        print('stop')
        self.pipe.send(b'stop')

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
            raise RuntimeError('Error setting exposure time: %s', andor.errors.get(ret, ''))
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
    
    '''
    @attribute(dtype=str)
    def simple_preamp_gain_control(self):
        ret = andor.get_enum_string(self.handle, 'SimplePreAmpGainControl')
        return ret
    
    @simple_preamp_gain_control.setter
    def simple_preamp_gain_control(self, value):
        andor.set_enum_string(self.handle, 'SimplePreAmpGainControl', value)
    '''
        
    @attribute(dtype=str)
    def TriggerMode(self):
        return self._trigger_mode
    
    @TriggerMode.setter
    def TriggerMode(self, value):
        andor.set_enum_string(self.handle, 'TriggerMode', value)
        self._trigger_mode = value
       
    @attribute(dtype=float)
    def FrameRate(self):
        return self._frame_rate
    
    @FrameRate.setter
    def FrameRate(self, value):
        ret = andor.sdk.AT_SetFloat(self.handle, 'FrameRate', value)
        if ret != 0:
            raise RuntimeError('Error setting FrameRate: %s', andor.errors.get(ret, ''))
        self._frame_rate = andor.get_float(self.handle, 'FrameRate')
        
    @attribute(dtype=float)
    def SensorTemperature(self):
        return andor.get_float(self.handle, 'SensorTemperature')
    
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
    '''
    dev_info = tango.DbDevInfo()
    dev_info._class = 'Andor3'
    dev_info.server = 'Andor3/test'
    dev_info.name = 'zyla/test/1'

    db = tango.Database()
    db.add_device(dev_info)
    '''
    Andor3.run_server()
    
