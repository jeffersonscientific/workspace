# NCL stuff:
#import Ngl, Nio
import Nio
#import pyncl
#
import pylab as plt
import numpy
#import scipy
#
import multiprocessing as mpp
import os,sys
import time
#import pathlib

import getpass
import subprocess
import contextlib
#
# TODO: include file.open() calls in batching. having the files open completely locks them down, to the degree
#  that even an ls on the directory hangs and hangs and hangs. i think this is the source of some mystery
#  failures.
# TODONE: added an "iosafer" version, that is a bit more careful with file open/close, but it is still a bit twitchy.
#  so next, let's wrap some "try, except" around the IO batches; give each batch say 10 tries. That should mitigate
#  some intermittent errors. between all of that, and then moving off the tool servers to dedicated resources
#  shold make a big difference. it would also be nice to (re-)build the netcdf tools to support parallel IO.
#
# write a function to script this process (of extracting a layer from the model data).
#. the data are some indexing of (time, altitude, lon, lat). This script will just focus on pulling
#. a complete altitude layer, and from that only the pressure (['U']) variable. We'll re-dim a little
#. bit, since we're just taking the one variable.
#
# NOTE: for now, let's skip the template file. It's a pain to validate, and may not really save us any time
#. anyway.
#def get_U_from_QBO_layer(layer_index=0, fpathname_src='', fpath_dest='', fname_dest=None,
#                         fpathname_template=None, kt_0=0, kt_max=None):
def copy_U_from_QBO_layer(layer_index=0, fpathname_src='', fpath_dest='', fname_dest=None, batch_size=None,
                         kt_0=0, kt_max=None, verbose=0, n_cpu=None):
    '''
    #
    # keep it simple for now. This script will basically do one job with only a few options.
    # Variables:
    # @layer_index: layer index to extract
    # @fnamename_src: full path abnd filename of source data file
    # @fpath_dest: output data file path. just the path; we want to allow for dynamic filename construction.
    # @fname_dest: output data filename
    # @fpathnamename_template: Full path an dname of a template file -- an empty (or otherwise intialized)
    #. file with the right dimensinos, etc.
    #.  writing the empty file seems to take a long, long time, so this is probably a reasonable way to save time.
    # @kt_0, @kt_max: start and stop time indices. They will default to 0, and {max}
    # 
    '''
    #
    # pseudo-recursive multi-threading:
    # there does not appear to be any benefit to multi-processing individual subsets, but we can fairly easily group multiple
    #  jobs. We can do this externally, or make it more transparent here. If we get a len(list-like)>1 input for layer_index
    #  and an appropriate n_cpu, set up a Pool() and push jobs back through this function (quasi-recursively) with
    # type(layer_index)=int and n_cpu=1
    #
    if hasattr(layer_index, '__len__') and len(layer_index)==1:
        # Just treat this as a scalar
        layer_index = layer_index[0]
    #
    if hasattr(layer_index, '__len__'):
        # if it's still list-like, do multiprocessing:
        # ... except it looks like we cannot MPP this at all, at least not with the Nio object; it appears that it does not
        #  support multi-threaded access at all. Can we use the netcdf libraries? Does Nio have an open_parallel() (or something)
        #  function?
        if n_cpu is None:
            n_cpu = mpp.cpu_count()
        #
        # we only need one core per layer:
        n_cpu = min(n_cpu, len(layer_index))
        #
        if verbose:
            print('*** instantiating Pool() with {} workers'.format(n_cpu))
        #
        P = mpp.Pool(n_cpu)
        #
        # we don't need to return anything, so we could probably use a map(), map_async(), or imap_{unordered?"). In the end, I think
        #  these are shorthands, something like map_async().get() is a wrapper around [apply_async()...]; [r.get() ]
        # Anyway, apply_async always works, it's easy to pass parameters to, etc.
        #  ... and in newer versions, this is achieved in starmap() and starmap_async()
        #
        #workers = [P.apply_async(copy_U_from_QBO_layer, kwds={'layer_index':k, 'fpathname_src':fpathname_src, 'fpath_dest':fpath_dest,
        #                 'batch_size':batch_size, 'kt_0':kt_0, 'kt_max':kt_max, 'verbose':verbose, 'n_cpu':1}) for k in layer_index]
        #P.close()
        #P.join()
        #jobs = P.starmap(copy_U_from_QBO_layer [[k, fpathname_src, fpath_dest, fname_dest, batch_size, kt_0, kt_max, verbose, 1]
        jobs = P.starmap(copy_U_from_QBO_layer, [[k, fpathname_src, fpath_dest, None, batch_size, kt_0, kt_max, verbose, 1] for k in layer_index])
        P.close()
        P.join()
        #
        # for now, hack out of this by just making this a serial loop:
        jobs = [copy_U_from_QBO_layer(k, fpathname_src, fpath_dest, None, batch_size, kt_0, kt_max, verbose, 1) for k in layer_index]
        return jobs


    #
    if fpathname_src is None:
        fpathname_src = "U_V_T_Z3_plWACCMSC_CTL_122.cam.h2.0001-0202.nc"
    if fpath_dest is None:
        fpath_dest = ''
    #
    # fname: we'll handle it later... Same for fname_template, kt_max
    #
    # fname_out template:
    # filo = "U_10_QBO_" + dum + ".nc"
    # (but we need to know more before we can )
    #
#     # try to validate the file. Just wrap this whole thing in a try, except. if we get an exception, 
#     #. we just (re-) create the file from scratch.
#     with contextlib.closing(Nio.open_file(fpathname_src, 'r')) as fin:
#         if not fpathname_template is None:
#             try:
#                 with contextlib.closing(Nio.open_file(fpathname_template), 'r') as f_temp:
#                     #
#                     # do we have a 'U' variable? We'll check the size. if the variable does not exist,
#                     #. we'll throw an excetion anyway.
#                     if numpy.shape(fin.variables['U']) != numpy.shape(f_temp.variables['U']):
#                         raise Exception('Shapes of variables[U] are not consistent')
#                     #
#                     # there are probably more efficient ways to do this, but this is logically very solid.
#                     #for ky,val in f_temp.dimensions.items():
#                     for ky in f_temp.variables['U'].dimensions:
#                         # check all dimensions, or just the ones we need?
#                         if not fin.dimensions[ky] == f_temp.dimensikons[ky]:
#                         #if not (ky in fin.dimensions.keys() and fin.dimensions[ky]==val):
#                             raise Exception('Dimensions of output template do not match input')
#                         #
#             except:
#                 print('Excepting on template file validation')
#                 fpathname_template = None
#                 #
#             #
    # create the output file:
    if verbose:
        t0=time.time()
        t1=t0
        t2=t1
    # 
    with contextlib.closing(Nio.open_file(fpathname_src, 'r')) as fin:
        # time dimension size:
        # we can get the full dimension size, if we are going to write the whole lot;
        # otherwise, maybe just a subset?
        n_time = fin.dimensions['time']
        if batch_size is None:
            batch_size=int(n_time/50)
        batch_size=int(batch_size)
        #
        if fname_dest is None or fname_dest=='':
            fname_dest = 'U_{}_QBO_k{}.nc'.format(fin.variables['lev_p'][layer_index], layer_index)
        fpathname_dest = os.path.join(fpath_dest, fname_dest)
        u_type = fin.variables['U'].datatype
        #
        os.system('rm {}'.format(fpathname_dest))
        if verbose:
            print('** DEBUG: open output file...')
            print('*** n_batches: ',n_time, batch_size, type(n_time), type(batch_size))
            n_batches = int(numpy.ceil(n_time/batch_size))
        #
        with contextlib.closing(Nio.open_file(fpathname_dest, 'c')) as fout:
            #
            fout.create_dimension('time', n_time)
            fout.create_dimension('lat', fin.dimensions['lat'])
            fout.create_dimension('lon', fin.dimensions['lon'])
            if verbose: print('** DEBUG: [{}:{}]:: dimensions created: {}'.format(os.getppid(), os.getpid(), time.time()-t0))
            #
            # set the dimension variable values as well:
            # NOTE: this step may not be necessary; these variables might be brought along by
            #. the principal data, since those data have lat, lon, time listed as their dimensions.
            for v_name in ('lat', 'lon', 'time'):
                fout.create_variable(v_name, u_type, (v_name, ) )
                fout.variables[v_name][:] = fin.variables[v_name]
                #
                if verbose: print('** DEBUG: [{}:{}]:: var {} created'.format(os.getppid(), os.getpid(), v_name))
            if verbose: print('** DEBUG: [{}:{}]:: variables created (cumulative time): {}'.format(os.getppid(), os.getpid(), time.time()-t0))
            #
            fout.create_variable('U','d',('time','lat','lon'))
            setattr(fout.variables['U'], 'standard_name', 'pressure')
            setattr(fout.variables['U'], 'units', 'kPa')
            if verbose: print('** DEBUG: [{}:{}]:: variable[U] created (cum time): {}'.format(os.getppid(), os.getpid(), time.time()-t0))
            #
            # NOTE: the .set_value() syntax is required for scaler, non-indexed values (or so I have read).
            #. we can also use it for arrays, but only if we are assigning the whole value -- in other words, the 
            #. dimensinos must match. (this works if dim(udum)==dim(fout)). I assume the exception is if the output
            #. dimension is undefined.
            #
            #fout.variables['u'].assign_value(udum)        # this works if dimensions align
            #fout.variables['u'][0:len(udum)] = udum      # use this for partial assignment (note, i think this does allow 
            #                                            # (aka, will expand) for overflow -- [k,j] > len(fout.variable) )
            #
            if verbose: print('DEBUG: [{}:{}]::file created: {} ({})'.format(os.getppid(), os.getpid(), fpathname_dest,  time.time()-t0))
            #
            # now write file:
            n_time = fin.dimensions['time']
            for j,k in enumerate(range(0, n_time, batch_size)):
                #print('** ** [{}:{}]'.format(k, k+batch_size_write))
                #
                # remember, our read-data are like:
                # udum = fin.variables['U'][0:batch_size, 2,:,:]
                # and we're making out output file more or less a mirror of that:
                # note: we never explicilty store the data in a local variable; the memory footprint
                #. is determined entirely by the NetCDF class, so really we can probably do this in one batch...
                #  unless we want to parallelize, in which case we can run each of thsed batches an a Process() thread,
                # in a Queue(), Pool(), etc.
                t1=t2
                t2 = time.time()
                k1 = min(k+batch_size, n_time)
                
                #
                #print('*** k:k1: {}:{}'.format(k,k1))
                fout.variables['U'][k:k1] = fin.variables['U'][k:k1, layer_index,:,:]
                #
                if verbose: print('** DEBUG: [{}:{}]:: begin  k={}/{} batches [{}:{}]/{} :: {}.'.format(os.getppid(), os.getpid(), j, n_batches, k, k1, n_time, t2-t1))
            #
        #
    return fpathname_dest
#
# a (possibly) IO safer way to do this...
def copy_U_from_QBO_layer_iosafer(layer_index=0, fpathname_src='', fpath_dest='', fname_dest=None, batch_size=None,
								  kt_0=0, kt_max=None, verbose=0, n_cpu=None):
    #
    '''
        #
        # keep it simple for now. This script will basically do one job with only a few options.
        # Variables:
        # @layer_index: layer index to extract
        # @fnamename_src: full path abnd filename of source data file
        # @fpath_dest: output data file path. just the path; we want to allow for dynamic filename construction.
        # @fname_dest: output data filename
        # @fpathnamename_template: Full path an dname of a template file -- an empty (or otherwise intialized)
        #. file with the right dimensinos, etc.
        #.  writing the empty file seems to take a long, long time, so this is probably a reasonable way to save time.
        # @kt_0, @kt_max: start and stop time indices. They will default to 0, and {max}
        #
        '''
    #
    # pseudo-recursive multi-threading:
    # there does not appear to be any benefit to multi-processing individual subsets, but we can fairly easily group multiple
    #  jobs. We can do this externally, or make it more transparent here. If we get a len(list-like)>1 input for layer_index
    #  and an appropriate n_cpu, set up a Pool() and push jobs back through this function (quasi-recursively) with
    # type(layer_index)=int and n_cpu=1
    #
    if verbose:
        print('** DEBUG[{}:{}] running IO-safe(r) version.'.format(os.getpid(), os.getppid()))
    
    #
    if hasattr(layer_index, '__len__') and len(layer_index)==1:
        # Just treat this as a scalar
        layer_index = layer_index[0]
    #
    if hasattr(layer_index, '__len__'):
        # if it's still list-like, do multiprocessing:
        # ... except it looks like we cannot MPP this at all, at least not with the Nio object; it appears that it does not
        #  support multi-threaded access at all. Can we use the netcdf libraries? Does Nio have an open_parallel() (or something)
        #  function?
        if n_cpu is None:
            n_cpu = mpp.cpu_count()
        #
        # we only need one core per layer:
        n_cpu = min(n_cpu, len(layer_index))
        #
        if verbose:
            print('*** instantiating Pool() with {} workers'.format(n_cpu))
        #
        P = mpp.Pool(n_cpu)
        #
        # we don't need to return anything, so we could probably use a map(), map_async(), or imap_{unordered?"). In the end, I think
        #  these are shorthands, something like map_async().get() is a wrapper around [apply_async()...]; [r.get() ]
        # Anyway, apply_async always works, it's easy to pass parameters to, etc.
        #  ... and in newer versions, this is achieved in starmap() and starmap_async()
        #
        #workers = [P.apply_async(copy_U_from_QBO_layer, kwds={'layer_index':k, 'fpathname_src':fpathname_src, 'fpath_dest':fpath_dest,
        #                 'batch_size':batch_size, 'kt_0':kt_0, 'kt_max':kt_max, 'verbose':verbose, 'n_cpu':1}) for k in layer_index]
        #P.close()
        #P.join()
        #jobs = P.starmap(copy_U_from_QBO_layer [[k, fpathname_src, fpath_dest, fname_dest, batch_size, kt_0, kt_max, verbose, 1]
        jobs = P.starmap(copy_U_from_QBO_layer, [[k, fpathname_src, fpath_dest, None, batch_size, kt_0, kt_max, verbose, 1] for k in layer_index])
        P.close()
        P.join()
        #
        # for now, hack out of this by just making this a serial loop:
        jobs = [copy_U_from_QBO_layer_iosafer(k, fpathname_src, fpath_dest, None, batch_size, kt_0, kt_max, verbose, 1) for k in layer_index]
        return jobs
    #
    #
    if fpathname_src is None:
        fpathname_src = "U_V_T_Z3_plWACCMSC_CTL_122.cam.h2.0001-0202.nc"
    if fpath_dest is None:
        fpath_dest = ''
    #
    # fname: we'll handle it later... Same for fname_template, kt_max
    #
    # fname_out template:
    # filo = "U_10_QBO_" + dum + ".nc"
    # (but we need to know more before we can )
    #
    #     # try to validate the file. Just wrap this whole thing in a try, except. if we get an exception,
    #     #. we just (re-) create the file from scratch.
    #     with contextlib.closing(Nio.open_file(fpathname_src, 'r')) as fin:
    #         if not fpathname_template is None:
    #             try:
    #                 with contextlib.closing(Nio.open_file(fpathname_template), 'r') as f_temp:
    #                     #
    #                     # do we have a 'U' variable? We'll check the size. if the variable does not exist,
    #                     #. we'll throw an excetion anyway.
    #                     if numpy.shape(fin.variables['U']) != numpy.shape(f_temp.variables['U']):
    #                         raise Exception('Shapes of variables[U] are not consistent')
    #                     #
    #                     # there are probably more efficient ways to do this, but this is logically very solid.
    #                     #for ky,val in f_temp.dimensions.items():
    #                     for ky in f_temp.variables['U'].dimensions:
    #                         # check all dimensions, or just the ones we need?
    #                         if not fin.dimensions[ky] == f_temp.dimensikons[ky]:
    #                         #if not (ky in fin.dimensions.keys() and fin.dimensions[ky]==val):
    #                             raise Exception('Dimensions of output template do not match input')
    #                         #
    #             except:
    #                 print('Excepting on template file validation')
    #                 fpathname_template = None
    #                 #
    #             #
    # create the output file:
    if verbose:
        t0=time.time()
        t1=t0
        t2=t1
    #
    with contextlib.closing(Nio.open_file(fpathname_src, 'r')) as fin:
        # time dimension size:
        # we can get the full dimension size, if we are going to write the whole lot;
        # otherwise, maybe just a subset?
        n_time = fin.dimensions['time']
        if batch_size is None:
            batch_size=int(n_time/50)
        batch_size=int(batch_size)
        #
        if fname_dest is None or fname_dest=='':
            fname_dest = 'U_{}_QBO_k{}.nc'.format(fin.variables['lev_p'][layer_index], layer_index)
        fpathname_dest = os.path.join(fpath_dest, fname_dest)
        #
        os.system('rm {}'.format(fpathname_dest))
        if verbose:
            print('** DEBUG: open output file...')
            n_batches = int(numpy.ceil(n_time/batch_size))
        n_time, n_pev, n_lat, n_lon = numpy.shape(fin.variables['U'])
        u_type = fin.variables['U'].datatype
    #
        with contextlib.closing(Nio.open_file(fpathname_dest, 'c')) as fout:
            #
            fout.create_dimension('time', n_time)
            fout.create_dimension('lat', n_lat)
            fout.create_dimension('lon', n_lon)
            if verbose: print('** DEBUG: [{}:{}]:: dimensions created: {}'.format(os.getppid(), os.getpid(), time.time()-t0))
            #
            # set the dimension variable values as well:
            # NOTE: this step may not be necessary; these variables might be brought along by
            #. the principal data, since those data have lat, lon, time listed as their dimensions.
            for v_name in ('lat', 'lon', 'time'):
                fout.create_variable(v_name, u_type, (v_name, ) )
                fout.variables[v_name][:] = fin.variables[v_name]
                #
                if verbose: print('** DEBUG: [{}:{}]:: var {} created'.format(os.getppid(), os.getpid(), v_name))
            if verbose: print('** DEBUG: [{}:{}]:: variables created (cumulative time): {}'.format(os.getppid(), os.getpid(), time.time()-t0))
    #
    with contextlib.closing(Nio.open_file(fpathname_dest, 'w')) as fout:
        fout.create_variable('U','d',('time','lat','lon'))
        setattr(fout.variables['U'], 'standard_name', 'pressure')
        setattr(fout.variables['U'], 'units', 'kPa')
        if verbose: print('** DEBUG: [{}:{}]:: variable[U] created (cum time): {}'.format(os.getppid(), os.getpid(), time.time()-t0))
        #
        #
        # now write file:
    for j,k in enumerate(range(0, n_time, batch_size)):
        #print('** ** [{}:{}]'.format(k, k+batch_size_write))
        #
        # remember, our read-data are like:
        # udum = fin.variables['U'][0:batch_size, 2,:,:]
        # and we're making out output file more or less a mirror of that:
        # note: we never explicilty store the data in a local variable; the memory footprint
        #. is determined entirely by the NetCDF class, so really we can probably do this in one batch...
        #  unless we want to parallelize, in which case we can run each of thsed batches an a Process() thread,
        # in a Queue(), Pool(), etc.
        t1=t2
        t2 = time.time()
        k1 = min(k+batch_size, n_time)
        with contextlib.closing(Nio.open_file(fpathname_src, 'r')) as fin:
            with contextlib.closing(Nio.open_file(fpathname_dest, 'w')) as fout:
                #
                #print('*** k:k1: {}:{}'.format(k,k1))
                fout.variables['U'][k:k1] = fin.variables['U'][k:k1, layer_index,:,:]
                if verbose: print('** DEBUG: [{}:{}]:: begin  k={}/{} batches [{}:{}]/{} :: {}.'.format(os.getppid(), os.getpid(), j, n_batches, k, k1, n_time, t2-t1))
        #
    #
    return fpathname_dest


#
if __name__ == '__main__':
	# get some input parameters...
	import argparse
    #import multiprocessing as mpp
	#
	# see argparse reff: https://docs.python.org/3.3/library/argparse.html
	#
	parser = argparse.ArgumentParser(description='Process some integers.')
	parser.add_argument('layer_indices', metavar='N', type=int, nargs='+',
                   help='indices of layers to process')
	parser.add_argument('--sum', dest='accumulate', action='store_const',
                   const=sum, default=max,
                   help='a toy function, to demonstrate argparse sum the integers (default: find the max)')
	# what does store_const do/mean?
	parser.add_argument('--io_safer', dest='f_QBO_copy', action='store_const',
						const=copy_U_from_QBO_layer_iosafer, default=copy_U_from_QBO_layer,
						help='Run copy_U_ in a (possibly) safer IO way, but probably a bit slower')
	# Options:
	# this one we just want to keep...
	parser.add_argument('--src_pathname', dest='src_pathname')
	parser.add_argument('--kt_0', dest='kt_0')
	parser.add_argument('--kt_max', dest='kt_max')
	parser.add_argument('--dest_path', dest='fpath_dest')
	parser.add_argument('--dest_fname', dest='fname_dest')
	parser.add_argument('--batch_size', dest='batch_size')
	parser.add_argument('--verbose', dest='verbose', default=0)
	parser.add_argument('--n_cpu', dest='n_cpu')
	#
	args = parser.parse_args()
	print('accumulate: ', args.accumulate(args.layer_indices))
	print('src_pathname: ', args.src_pathname)
	print('indices: ', args.layer_indices)
	#
	layer_indices = args.layer_indices
	if len(layer_indices)==1:
		layer_indices=layer_indices[0]
	kt_max = args.kt_max
	#print('*** kt_max: ', args.kt_max)
	#
	# we'll call the parsing function like this:
	# def copy_U_from_QBO_layer(layer_index=0, fpathname_src='', fpath_dest='', fname_dest=None, batch_size=None,
	#		kt_0=0, kt_max=None, verbose=0, n_cpu=None):
    # Note: we have added quasi-recuersive parallelization to copy_U_from_QBO_layer(), so all we do here is pass variables...
	#x = copy_U_from_QBO_layer(layer_index=layer_indices, fpathname_src=args.src_pathname, fpath_dest=args.fpath_dest, fname_dest=args.fname_dest, batch_size=args.batch_size,
	x = args.f_QBO_copy(layer_index=layer_indices, fpathname_src=args.src_pathname, fpath_dest=args.fpath_dest, fname_dest=args.fname_dest, batch_size=args.batch_size, kt_0=args.kt_0, kt_max=args.kt_max, verbose=args.verbose, n_cpu=args.n_cpu)
