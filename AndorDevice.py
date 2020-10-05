import os
import zmq
import andor 
import weakref
import tango
import numpy as np
from threading import Thread
from tango import DevState
from tango.server import Device, attribute, command, run, device_property

class Andor3Device(Device):
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
        self.data_socket = self.context.socket(zmq.PUB)
        self.data_socket.bind('tcp://*:9999')
        self._filename = ''
        self._frame_count = 1
        
        andor.set_enum_string(self.handle, 'TriggerMode', 'Internal')
        andor.set_enum_string(self.handle, 'CycleMode', 'Fixed')

        image_size = andor.get_int(self.handle, 'ImageSizeBytes')
        print('ImageSizeBytes', image_size)
        self.buffers = []
        for i in range(100):
            buf = np.empty(image_size, np.uint8)
            self.buffers.append(buf)
            
        height = andor.get_int(self.handle, 'AOIHeight')
        width = andor.get_int(self.handle, 'AOIWidth')
        stride = andor.get_int(self.handle, 'AOIStride')
        print(height, width, stride)
        
        self.thread = Thread(target=self.main)
        self.thread.start()
        
    def init_device(self):
        self.set_change_event('state', True, False)
        self.set_state(DevState.ON)
        
    def queue_buffer(self, buf, size):
        print('cleanup')
        andor.sdk.AT_QueueBuffer(self.handle, buf, size)
    
    def main(self):
        pipe = self.context.socket(zmq.PAIR)
        pipe.connect('inproc://zyla')
        fd_video = os.open('/dev/video0', os.O_RDONLY)
        poller = zmq.Poller()
        poller.register(fd_video, zmq.POLLIN)
        poller.register(pipe, zmq.POLLIN)
        last_frame = zmq.Frame()
        acquired_frames = 0
        running = False
        while True:
            events = dict(poller.poll())
            if fd_video in events and events[fd_video] == zmq.POLLIN:
                img = andor.wait_buffer(self.handle, 0)
                if img is None:
                    continue
                buf, size = img
                print('frame', acquired_frames)
                frame = zmq.Frame(andor.ffi.buffer(buf, size), copy=False)
                weakref.finalize(frame, self.queue_buffer, buf, size)
                last_frame = frame
                self.data_socket.send_json({'htype': 'image',
                                  'frame': acquired_frames,
                                  'shape': [self.height, self.width],
                                  'type': 'int16',
                                  'compression': 'none'}, flags=zmq.SNDMORE)
                self.data_socket.send(frame, copy=False)
                acquired_frames += 1
                if acquired_frames == self.frame_count:
                    self.data_socket.send_json({'htype': 'series_end'})
                    andor.sdk.AT_Command(self.handle, 'AcquisitionStop')
                    andor.sdk.AT_Flush(self.handle)
                    acquired_frames = 0
                
            if pipe in events and events[pipe] == zmq.POLLIN:
                msg = pipe.recv()
                if msg == b'start':
                    running = True
                    self.data_socket.send_json({'htype': 'header',
                                                'filename': self._filename})
                elif msg == b'stop':
                    if running:
                        self.data_socket.send_json({'htype': 'series_end'})
                    andor.sdk.AT_Command(self.handle, 'AcquisitionStop')
                    andor.sdk.AT_Flush(self.handle)
                    acquired_frames = 0
                    running = False
            
    @command
    def start(self):
        print('start')
        self.height = andor.get_int(self.handle, 'AOIHeight')
        self.width = andor.get_int(self.handle, 'AOIWidth')
        self.pipe.send(b'start')
        for buf in self.buffers:
            andor.sdk.AT_QueueBuffer(self.handle, andor.ffi.from_buffer(buf), buf.nbytes)
        andor.sdk.AT_Command(self.handle, 'AcquisitionStart')
        
    @command
    def stop(self):
        print('stop')
        self.pipe.send(b'stop')
        
    @command
    def test(self):
        print('test')
        self.pipe.send(b'stop')
        
    @attribute(dtype=str)
    def filename(self):
        return self._filename
    
    @filename.setter
    def filename(self, value):
        self._filename = value
        
    @attribute(dtype=int)
    def frame_count(self):
        return self._frame_count
    
    @frame_count.setter
    def frame_count(self, value):
        andor.sdk.AT_SetInt(self.handle, 'FrameCount', value)
        self._frame_count = value
        
    @attribute(dtype=float)
    def exposure_time(self):
        return andor.get_float(self.handle, 'ExposureTime')
    
    @exposure_time.setter
    def exposure_time(self, value):
        ret = andor.sdk.AT_SetFloat(self.handle, 'ExposureTime', value)
        if ret != 0:
            raise RuntimeError('Error setting exposure time: %s', andor.errors.get(ret, ''))
        
        
if __name__ == '__main__':
    
    #dev_info = tango.DbDevInfo()
    #dev_info._class = 'Andor3Device'
    #dev_info.server = 'Andor3Device/test'
    #dev_info.name = 'zyla/test/1'

    #db = tango.Database()
    #db.add_device(dev_info)
    
    Andor3Device.run_server()
