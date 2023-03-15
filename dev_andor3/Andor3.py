import os
import zmq
import tango
import signal
import numpy as np
from functools import wraps
from threading import Thread
from tango import DevState, AttrWriteType
from tango.server import Device, attribute, command, run, device_property
from libdaq import Client, Receiver
from . import andor
from . import atutility

def handle_error(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            msg = '%s failed: %s' %(func.__name__, e)
            self._error_msg = msg
            self.error_stream(msg)
            raise e
            
    return wrapper

trigger_map = {"Internal": "INTERNAL", "External": "EXTERNAL_MULTI", "Software": "SOFTWARE"}

class Andor3(Device):
    receiver_host = device_property(dtype=str, mandatory=True)
    receiver_port = device_property(dtype=int, mandatory=True)
    k8s_namespace = device_property(dtype=str)

    SimplePreAmpGainControl = attribute(dtype=str,
                                        access=AttrWriteType.READ_WRITE)
    TriggerMode = attribute(dtype=str,
                            access=AttrWriteType.READ_WRITE)
    DestinationFilename = attribute(dtype=str,
                                    access=AttrWriteType.READ_WRITE)

    nTriggers = attribute(dtype=int,
                          access=AttrWriteType.READ_WRITE)

    FrameRate = attribute(dtype=float,
                          access=AttrWriteType.READ_WRITE)

    ScanConfig = attribute(dtype=str,
                          access=AttrWriteType.READ_WRITE)

    def __init__(self, *args, **kwargs):
        self.context = zmq.Context()
        self.pipe = self.context.socket(zmq.PAIR)
        self.pipe.bind('inproc://zyla')

        # this internally calls init_device
        super().__init__(*args, **kwargs)

        self.register_signal(signal.SIGINT)

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
        self._error_msg = ''
        self._armed = False
        self._frame_count = 1
        self._acquired_frames = 0
        # 0 is idle and 1 is running
        self._running = 0
        self._fliplr = False
        self._flipud = False
        self._rotation = 0
        self._scan_config = None
        
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

        self._target_temperature = None
        if andor.is_implemented("TargetSensorTemperature"):
            self._target_temperature = andor.get_float(self.handle, 'TargetSensorTemperature')

        atutility.sdk.AT_InitialiseUtilityLibrary()

        self._gain_control = andor.get_enum_string(self.handle, 'SimplePreAmpGainControl')
        self.write_SimplePreAmpGainControl(self._gain_control)

        #print(andor.get_enum_string_options(self.handle, 'TemperatureControl'))
        
        options = andor.get_enum_string_options(self.handle, 'SimplePreAmpGainControl')
        self._gain_control_options = '\n'.join(options)
        
        andor.set_enum_string(self.handle, 'CycleMode', 'Fixed')
        
        self.buffers = []

        self.receiver = Receiver(self.context, self.receiver_host, self.receiver_port, 'streaming-receiver')

        self.set_state(DevState.ON)
    
    def delete_device(self):
        print('delete_device')
        andor.sdk.AT_Close(self.handle)
        self.set_state(DevState.OFF)


    def signal_handler(self, signo):
        self.pipe.send(b'terminate')
        self.thread.join(1)

    @handle_error
    def update_state_and_status(self):
        if self._error_msg:
            self.set_state(DevState.FAULT)
            self.set_status(self._error_msg)
            return 
        
        receiver_status = self.receiver.status()
        if self._running == 1:
            state, status = DevState.RUNNING, 'Acquisition in progress'
            
        elif receiver_status['state'] == 'error':
            state, status = DevState.FAULT, 'Error from streaming-receiver: %s' % receiver_status['error']
            
        elif self._armed:
            if receiver_status['state'] == 'running':
                state, status = DevState.RUNNING, 'Waiting for streaming-receiver to finish'
            else:
                self._armed = False
                state, status = DevState.ON, 'Idle'
                
        else:
            state, status = DevState.ON, 'Idle'

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
        self.data_socket = self.context.socket(zmq.PUSH)
        self.data_socket.bind(os.environ.get("DATA_SOCKET", 'tcp://*:9999'))
        self._msg_number = 0
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
                                                "scan": self._scan_config,
                                                #"scan": [{"scanvar": i, "nTriggers": 10} for i in np.linspace(-10e-9, 10e-9, 100)]
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
        self.buffers.clear()
        for i in range(100):
            buf = np.empty(image_size, np.uint8)
            self.buffers.append(buf)
        andor.sdk.AT_Flush(self.handle)
        for buf in self.buffers:
            andor.sdk.AT_QueueBuffer(self.handle, andor.ffi.from_buffer(buf), image_size)
        self.pipe.send(b'start')
        if not self.receiver.wait_for_running(5.0):
            raise RuntimeError('No reply from streaming-receiver after Arm')
        andor.sdk.AT_Command(self.handle, 'AcquisitionStart')
        self._armed = True

    @command
    def Live(self):
        self.write_DestinationFilename('')
        self.write_nTriggers(100000)
        self.write_TriggerMode('INTERNAL')
        self.write_FrameRate(1)
        self.Arm()

    @command
    def SoftwareTrigger(self):
        andor.sdk.AT_Command(self.handle, 'SoftwareTrigger')
        
    @command
    def Stop(self):
        print('stop')
        self.pipe.send(b'stop')
        
    @command
    def RestartReceiver(self):
        if self.k8s_namespace:
            self.receiver.restart(self.k8s_namespace)
        
    @attribute(dtype=str)
    def CameraModel(self):
        return self._camera_model

    @attribute(dtype=int)
    def nFramesAcquired(self):
        return self._acquired_frames

    @attribute(dtype=int)
    def nFramesReceived(self):
        return self.receiver.frames_received

    def read_DestinationFilename(self):
        return self._filename
    
    def write_DestinationFilename(self, value):
        self._filename = value

    def read_ScanConfig(self):
        return self._scan_config

    def write_ScanConfig(self, value):
        self._scan_config = value
        
    def read_nTriggers(self):
        return self._frame_count
    
    def write_nTriggers(self, value):
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
    
    def read_SimplePreAmpGainControl(self):
        ret = andor.get_enum_string(self.handle, 'SimplePreAmpGainControl')
        return ret
    
    def write_SimplePreAmpGainControl(self, value):
        andor.set_enum_string(self.handle, 'SimplePreAmpGainControl', value)
        if "12-bit" in value:
            andor.set_enum_string(self.handle, 'PixelEncoding', "Mono12Packed")


    @attribute(dtype=str)
    def SimplePreAmpGainControlOptions(self):
        return self._gain_control_options
        
    def read_TriggerMode(self):
        return trigger_map[self._trigger_mode]
    
    def write_TriggerMode(self, value):
        val = {v:k for k,v in trigger_map.items()}[value]
        andor.set_enum_string(self.handle, 'TriggerMode', val)
        self._trigger_mode = val
       
    def read_FrameRate(self):
        return andor.get_float(self.handle, 'FrameRate')
    
    def write_FrameRate(self, value):
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
    def TargetSensorTemperature(self):
        return self._target_temperature

    @ElectronicShutteringMode.setter
    def TargetSensorTemperature(self, value):
        if self._target_temperature is not None:
            andor.set_float(self.handle, 'TargetSensorTemperature', value)
            self._target_temperature = value
        
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
    def ReadoutTime(self):
        return andor.get_float(self.handle, 'ReadoutTime')

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
    
    #dev_info = tango.DbDevInfo()
    #dev_info._class = 'Andor3'
    #dev_info.server = 'Andor3/b309a-e01'
    #dev_info.name = 'b309a-e01/dia/zyla'

    #db = tango.Database()
    #db.add_device(dev_info)
    
    Andor3.run_server()
    
#main()
