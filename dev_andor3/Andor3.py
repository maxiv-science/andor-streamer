import os
import zmq
import json
import tango
import signal
import numpy as np
import logging
from functools import wraps
from threading import Thread
from tango import DevState, AttrWriteType
from tango.server import Device, attribute, command, run, device_property
from libdaq import Client, Receiver
from . import andor
from . import atutility

logging.basicConfig()

logger = logging.getLogger(__name__)

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
    receiver_url = device_property(dtype=str, mandatory=True)
    data_port = device_property(dtype=int, default_value=9999)
    k8s_namespace = device_property(dtype=str)
    serial_number = device_property(dtype=str, default_value="")

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

    ExposureTime = attribute(dtype=float,
                          access=AttrWriteType.READ_WRITE)

    ElectronicShutteringMode = attribute(dtype=str,
                             access=AttrWriteType.READ_WRITE)
    
    PresetExposureTime = device_property(dtype=float)

    PresetElectronicShutteringMode = device_property(dtype=str)

    PresetSimplePreAmpGainControl = device_property(dtype=str)

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
        print('Found %d devices', devcount)
        i = 0
        if self.serial_number != "":
            snmap = {}
            for i in range(devcount):
                handle = andor.ffi.new('AT_H*')
                andor.sdk.AT_Open(i, handle)
                camera_model = andor.get_string(handle[0], 'CameraModel')
                camera_serial = andor.get_string(handle[0], 'SerialNumber')
                print('CameraModel %s: serial: %s', camera_model, camera_serial)
                andor.sdk.AT_Close(handle[0])
                snmap[camera_serial] = i
            i = snmap[self.serial_number]


        handle = andor.ffi.new('AT_H*')
        andor.sdk.AT_Open(i, handle)
        self.handle = handle[0]
        self._camera_model = andor.get_string(self.handle, 'CameraModel')
        self._camera_serial = andor.get_string(handle[0], 'SerialNumber')

        print("using serial number", self._camera_serial)
        if "SIMCAM" in self._camera_model:
            logger.error("only simcam found. make sure to have camera connected and on.")
            self._error_msg = "only simcam found. make sure to have camera connected and on."
            self.set_state(DevState.FAULT)
            return

        # find correct dev/video number
        import glob
        vdevs = glob.glob("/dev/video*")
        print("video devices", vdevs)

        fdmap = {}
        poller = zmq.Poller()
        for vdev in vdevs:
            fd_video = os.open(vdev, os.O_RDONLY)
            fdmap[fd_video] = vdev
            poller.register(fd_video, zmq.POLLIN)

        self.buffers = []

        image_size = andor.get_int(self.handle, 'ImageSizeBytes')
        self.buffers.clear()
        for i in range(100):
            buf = np.empty(image_size, np.uint8)
            self.buffers.append(buf)
        andor.sdk.AT_Flush(self.handle)
        for buf in self.buffers:
            andor.sdk.AT_QueueBuffer(self.handle, andor.ffi.from_buffer(buf), image_size)
        andor.sdk.AT_Command(self.handle, 'AcquisitionStart')

        polled = dict(poller.poll())
        print("polled data: ", polled)
        print("map", fdmap)

        polledfds = list(polled.keys())
        if len(polledfds) != 1:
            self.set_state(DevState.FAULT)
            self.set_status("no corresponding video device found")
            return

        self.videodevice = fdmap[polledfds[0]]
        print("using video device", self.videodevice)

        andor.sdk.AT_Command(self.handle, 'AcquisitionStop')
        andor.sdk.AT_Flush(self.handle)

        for fd in fdmap:
            poller.unregister(fd)
            os.close(fd)

        self._filename = ''
        self._label = ''
        self._nproj = 1
        self._save_raw = True
        self._error_msg = ''
        self._armed = False
        self._frame_count = 1
        self._acquired_frames = 0
        # 0 is idle and 1 is running
        self._running = 0
        self._fliplr = False
        self._flipud = False
        self._rotation = 0

        self._exposure_time = andor.get_float(self.handle, 'ExposureTime')
        if self.PresetExposureTime:
            print("setting PresetExposureTime", self.PresetExposureTime)
            self.write_ExposureTime(self.PresetExposureTime)
        self._trigger_mode = andor.get_enum_string(self.handle, 'TriggerMode')
        self._shutter_mode = andor.get_enum_string(self.handle, 'ElectronicShutteringMode')
        if self.PresetElectronicShutteringMode:
            print("setting PresetElectronicShutteringMode", self.PresetElectronicShutteringMode)
            self.write_ElectronicShutteringMode(self.PresetElectronicShutteringMode)
        self._pixel_readout_rate = andor.get_enum_string(self.handle, 'PixelReadoutRate')
        self._sensor_cooling = andor.get_bool(self.handle, 'SensorCooling')
        self._width = andor.get_int(self.handle, 'AOIWidth')
        self._left = andor.get_int(self.handle, 'AOILeft')
        self._height = andor.get_int(self.handle, 'AOIHeight')
        self._top = andor.get_int(self.handle, 'AOITop')

        self._target_temperature = None
        if andor.is_implemented(self.handle, "TargetSensorTemperature"):
            self._target_temperature = andor.get_float(self.handle, 'TargetSensorTemperature')

        atutility.sdk.AT_InitialiseUtilityLibrary()

        if self.PresetSimplePreAmpGainControl:
            print("setting PresetSimplePreAmpGainControl", self.PresetSimplePreAmpGainControl)
            self.write_SimplePreAmpGainControl(self.PresetSimplePreAmpGainControl)
        self._gain_control = andor.get_enum_string(self.handle, 'SimplePreAmpGainControl')
        self.write_SimplePreAmpGainControl(self._gain_control)

        #print(andor.get_enum_string_options(self.handle, 'TemperatureControl'))
        
        options = andor.get_enum_string_options(self.handle, 'SimplePreAmpGainControl')
        self._gain_control_options = '\n'.join(options)
        
        andor.set_enum_string(self.handle, 'CycleMode', 'Fixed')
        
        self.buffers = []

        self.receiver = Receiver(self.receiver_url)

        self.set_state(DevState.ON)
    
    def delete_device(self):
        logger.info('delete_device')
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
        self.data_socket.bind(os.environ.get("DATA_SOCKET", f'tcp://*:{self.data_port}'))
        self._msg_number = 0
        fd_video = os.open(self.videodevice, os.O_RDONLY)
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
                while ret := andor.wait_buffer(self.handle, 0):
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
                    logger.debug('start acquisition')
                    self._running = 1
                    self.data_socket.send_json({'htype': 'header',
                                                'filename': self._filename,
                                                'msg_number': self._msg_number}, flags=zmq.SNDMORE)
                    
                    meta = {'cooling': self._sensor_cooling,
                            'label': self._label,
                            'nproj': self._nproj,
                            'save_raw': self._save_raw
                    }
                    self.data_socket.send_json(meta)
                    self._msg_number += 1
                elif msg == b'stop':
                    logger.debug('end acquisition')
                    finish()

                elif msg == b'terminate':
                    logger.debug('terminating network thread')
                    finish()
                    break
            
            
    @command
    def Arm(self):
        logger.info('start nTriggers %d', self._frame_count)
        self.stride = andor.get_int(self.handle, 'AOIStride')
        self.pixel_encoding = andor.get_enum_string(self.handle, 'PixelEncoding')
        logger.debug("height %d, width %d, stride %d, encoding %s", self._height, self._width, self.stride, self.pixel_encoding)
        logger.info('ReadoutTime %f', andor.get_float(self.handle, 'ReadoutTime'))
        logger.debug('ImageSizeBytes %d', andor.get_int(self.handle, 'ImageSizeBytes'))
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
        maxfr = andor.get_float_max(self.handle, 'FrameRate')
        self.write_FrameRate(min(maxfr, 1))
        self.Arm()

    @command
    def Continuous(self):
        self.write_DestinationFilename('')
        self.write_nTriggers(100000)
        self.write_TriggerMode('EXTERNAL_MULTI')
        self.Arm()

    @command
    def SoftwareTrigger(self):
        andor.sdk.AT_Command(self.handle, 'SoftwareTrigger')
        
    @command
    def Stop(self):
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

    def read_nTriggers(self):
        return self._frame_count
    
    def write_nTriggers(self, value):
        andor.sdk.AT_SetInt(self.handle, 'FrameCount', value)
        self._frame_count = value

    def read_ExposureTime(self):
        return self._exposure_time
    
    def write_ExposureTime(self, value):
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
        
    def read_ElectronicShutteringMode(self):
        return self._shutter_mode
        
    def write_ElectronicShutteringMode(self, value):
        andor.set_enum_string(self.handle, 'ElectronicShutteringMode', value)
        self._shutter_mode = value

    @attribute(dtype=float)
    def TargetSensorTemperature(self):
        return self._target_temperature

    @TargetSensorTemperature.setter
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
    
    
    # Attributes for the realtime tomo pipeline
    @attribute(dtype=str)
    def Label(self):
        return self._label
    
    @Label.setter
    def Label(self, value):
        self._label = value
    
    @attribute(dtype=int)
    def Nproj(self):
        return self._nproj
    
    @Nproj.setter
    def Nproj(self, value):
        self._nproj = value

    # Attributes for the data reduction pipeline

    @attribute(dtype=bool)
    def SaveRaw(self):
        return self._save_raw

    @SaveRaw.setter
    def SaveRaw(self, value):
        self._save_raw = value
    
    # ROI attributes
        
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

if __name__ == "__main__":
    main()
