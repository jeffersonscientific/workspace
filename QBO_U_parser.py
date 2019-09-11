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
'''
# CODE SYNOPSIS:
# write a function to script this process (of extracting a layer from the model data).
#. the data are some indexing of (time, altitude, lon, lat). This script will just focus on pulling
#. a complete altitude layer, and from that only the pressure (['U']) variable. We'll re-dim a little
#. bit, since we're just taking the one variable.
'''
#
# NOTE: included file.open() calls in batching. having the files open completely locks them down, to the degree
#  that even an ls on the directory hangs and hangs and hangs. i think this is the source of some mystery
#  failures.
# TODONE: added an "iosafer" version, that wraps files open/close around the copy batches, but it is still a bit twitchy.
#  so next, let's wrap some "try, except" around the IO batches; give each batch say 10 tries. That should mitigate
#  some intermittent errors. between all of that, and then moving off the tool servers to dedicated resources
#  shold make a big difference. it would also be nice to (re-)build the netcdf tools to support parallel IO.
#  Note that this does not mitigate the problems with the initial read/write of variables['time']... though
#  maybe we can batch that too?
# NOTE: These IO wrappers do cost us some performance, but they dramatically enhance stability, so probably worth
#  the effort.
#
#
# NOTE: for now, let's skip the template file. It's a pain to validate, and may not really save us any time
#. anyway.
#def get_U_from_QBO_layer(layer_index=0, fpathname_src='', fpath_dest='', fname_dest=None,
#                         fpathname_template=None, kt_0=0, kt_max=None):
def copy_U_from_QBO_layer(layer_index=0, fpathname_src='', fpath_dest='', fname_dest=None, batch_size=None,
                         kt_0=0, kt_max=None, verbose=0, n_cpu=None, n_tries=10):
    '''
    #
    # keep it simple(ish) for now. This script will basically do one job with only a few options.
    #
    # That job is to copy one layer of a 4-d (t, x,y,z) atmospheric model output to a separate file
    #  for simplified analysis. The input file is on order ~250GB; the output files should be about
    #  ~7.5 GB. This code uses the NCL Nio library, though the NetCDF4 library might be a better choice.
    #  Certainly, using libraries and data objects in which parallel IO is enabled is/would be a big plus
    #  as well (so that' something to work on).
    #
    # One of the main challenges is that the whole process is prone to failure when files are locked open,
    #  and the IO is generally slow. We have implemented two versions of the basic script. The first -- simpler
    #  in principle faster, version opens both the input and output files and keeps them open while it copies variables.
    #  note, we have implmeneted batch copying (copy a series of small subsets). We determined experimentally that
    #  this significantly improves performance. The second version wraps the file-open/close blocks into these batches,
    #  so the file is frequently opened and closed during the copy process. This costs a bit of performance, but makes
    #  the process much more tolerant to interruption.
    #
    # The basic operation (which is complicated a little bit by fault tolerance, etc. details):
    #    - Open the source file
    #        - read some structural information, meta-data, etc.
    #        - Open the output file
    #          - Create necessary variables (copy their structure from the input file and meta-data)
    #          - Copy dimension data from input to output
    #          - Copy data data from input to output
    #        - Close output
    #    - Close input
    #
    # NOTE: We wrote in hooks for Python multi-processing, but it turns out that this process is so IO bound, and
    #  and the IO (in current configurations) is so bound up and not thread friendly, that it does a lot more harm
    #  than good. So we (probably) have rewritten the mpp hooks (a quasi-recursive approach) to be explicitly serial.
    #  Obviously, serial operation can also be achieved in a qsub (or sbatch) script.
    #
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
        layer_index = int(layer_index[0])
    #
    # If we encounter an error or an exception tring to copy a block, how many times to we try again?
    if n_tries is None:
        n_tires=10
    n_tries = int(n_tries)
    #
    if hasattr(layer_index, '__len__'):
        # if it's still list-like, do multiprocessing:
        # ... except it looks like we cannot MPP this at all, at least not with the Nio object; it appears that it does not
        #  support multi-threaded access at all. Can we use the netcdf libraries? Does Nio have an open_parallel() (or something)
        #  function?
        layer_index = [int(x) for x in layer_index]
        if n_cpu is None:
            n_cpu = mpp.cpu_count()
        n_cpu = int(n_cpu)
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
#                 'batch_size':batch_size, 'kt_0':kt_0, 'kt_max':kt_max, 'verbose':verbose, 'n_cpu':1, 'n_tries':n_tries}) for k in layer_index]
        #P.close()
        #P.join()
        #jobs = P.starmap(copy_U_from_QBO_layer [[k, fpathname_src, fpath_dest, fname_dest, batch_size, kt_0, kt_max, verbose, 1, n_tries]
        jobs = P.starmap(copy_U_from_QBO_layer, [[k, fpathname_src, fpath_dest, None, batch_size, kt_0, kt_max, verbose, 1, n_tries] for k in layer_index])
        P.close()
        P.join()
        #
        # for now, hack out of this by just making this a serial loop:
        jobs = [copy_U_from_QBO_layer(k, fpathname_src, fpath_dest, None, batch_size, kt_0, kt_max, verbose, 1, n_tries) for k in layer_index]
        #
        return jobs
    #
    #######################
    # SPP job:
    #
    if fpathname_src is None:
        fpathname_src = "U_V_T_Z3_plWACCMSC_CTL_122.cam.h2.0001-0202.nc"
    if fpath_dest is None:
        fpath_dest = ''
    #
    # debugging variables...
    if verbose:
        t0=time.time()
        t1=t0
        t2=t1
    #
    # use a context manager to open the file(s):
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
        #u_type = fin.variables['U'].typecode()
        #
        os.system('rm {}'.format(fpathname_dest))
        if verbose:
            print('** DEBUG: open output file...')
            print('*** n_batches: ',n_time, batch_size, type(n_time), type(batch_size))
            n_batches = int(numpy.ceil(n_time/batch_size))
        #
        with contextlib.closing(Nio.open_file(fpathname_dest, 'c')) as fout:
            #
            # create dimensions:
            fout.create_dimension('time', n_time)
            fout.create_dimension('lat', fin.dimensions['lat'])
            fout.create_dimension('lon', fin.dimensions['lon'])
            if verbose: print('** DEBUG: [{}:{}]:: dimensions created: {}'.format(os.getppid(), os.getpid(), time.time()-t0))
            #
            # set the dimension variable values as well:
            # NOTE: this step may not be necessary; these variables might be brought along by
            #. the principal data, since those data have lat, lon, time listed as their dimensions.
            #
            # Create variables:
            # we can combine variable creation and assignment, but since it is super fast to create all of the variables,
            #  it sort of makes more sense to separeate the create/assign processes.
            # separate variable creation and assignment.
            # Create dimension variables:
            for v_name in ('lat', 'lon', 'time'):
                fout.create_variable(v_name, fin.variables[v_name].typecode(), (v_name, ) )
            #
            # Create data variable(s):
            fout.create_variable('U',fin.variables['U'].typecode(),('time','lat','lon'))
            setattr(fout.variables['U'], 'standard_name', 'pressure')
            setattr(fout.variables['U'], 'units', 'kPa')
            if verbose: print('** DEBUG: [{}:{}]:: variable[U] created (cum time): {}'.format(os.getppid(), os.getpid(), time.time()-t0))
            #
            # Assign values (note, we'll assign dimension variables).
            # Note, writing U is messy, because we want only one layer, U[:,layer_index,:,:], so we'll just do it separately.
            for v_name in ('lat', 'lon', 'time'):
                len_var = len(fout.variables[v_name])
                #
                # the time variable is really, really big, and so takes forever and would benefit from batching and exception handling.
                for k in range(0,len_var, batch_size):
                    #
                    k1 = min(k+batch_size, n_time)
                    if verbose:
                        print('*** DEBUG: assigning variables[{}]:: {}/{}//{} '.format(v_name,k, k1, len_var))
                    # add some exception handling:
                    for k_try in range(n_tries):
                        try:
                            fout.variables[v_name][k:k1] = (fin.variables[v_name])[k:k1]
                            if (verbose and len_var>batch_size):
                                t2 = time.time()
                                print('** DEBUG: variables[{}]: {}-{}/{} copied, dt={}'.format(v_name, k,min(k+batch_size, len_var), len_var, t2-t1))
                                t1=t2
                                #
                            # if we made it this far, it worked, so exit the try for-loop
                            break
                        except Exception as e:
                            if k_try>=n_tries-1:
                                raise Exception('*** EXCEPTION: Too many failures to write U[{}:{}]'.format(k,k1))
                                print('*** Exception Message: {}'.format(e))
                            else:
                                print('*** Warning! Failed {}/{} times to write variables[{}][{}:{}]'.format(v_name, k_try, n_tries, k,k1))
                #
                if verbose: print('** DEBUG: [{}:{}]:: var {} created'.format(os.getppid(), os.getpid(), v_name))
            if verbose: print('** DEBUG: [{}:{}]:: variables created (cumulative time): {}'.format(os.getppid(), os.getpid(), time.time()-t0))
            #

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
            if verbose: print('DEBUG: [{}:{}]::Template file created: {} ({})'.format(os.getppid(), os.getpid(), fpathname_dest,  time.time()-t0))
            #
            # now write data to output file:
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
                if verbose:
                    t1=t2
                    t2 = time.time()
                #
                k1 = min(k+batch_size, n_time)
                #
                # Add some exception handling:
                # use a boolean "success" flag:
                try_task_completed = False
                for k_try in range(n_tries):
                    try:
                        fout.variables['U'][k:k1] = fin.variables['U'][k:k1, layer_index,:,:]
                        #
                        # NOTE: assigning this variable, the break, etc. could also be placed in the else: clause of try:, except:..., else:
                        try_task_completed = True
                        #
                        if verbose:
                            t2=time.time()
                            print('** DEBUG: writing U:: [{}:{}]:: begin  k={}/{} batches [{}:{}]/{} :: {}.'.format(os.getppid(), os.getpid(), j, n_batches, k, k1, n_time, t2-t1))
                            t1=t2
                        break
                    except Exception as e:
                        if k_try >= (n_tries-1):
                            raise Exception('*** EXCEPTION: Too many failures to write U[{}:{}]'.format(k,k1))
                            print('*** Exception Message: {}'.format(e))
                        else:
                            print('*** Warning! Failed {}/{} times to write U[{}:{}]'.format(k_try, n_tries, k,k1))
                        #
                    #
                # this "if not" should work but arguably, we should use an assertion:
                #if not try_task_completed:
                #    raise Exception('*** EXCEPTION (trapped by try_task_completed): Too many failures to write U[{}:{}]'.format(k,k1))
                assert try_task_completed,'*** EXCEPTION (trapped by try_task_completed): Too many failures to write U[{}:{}]'.format(k,k1)
            if verbose:
                print('** DEBUG: QBO complete. Elapsed time: {}'.format(time.time()-t0))
        #
    return fpathname_dest
#
# a (possibly) IO safer way to do this...
def copy_U_from_QBO_layer_iosafer(layer_index=0, fpathname_src='', fpath_dest='', fname_dest=None, batch_size=None,
								  kt_0=0, kt_max=None, verbose=0, n_cpu=None, n_tries=10):
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
        # @verbose: debugging level. 0 is None. 1 or "True" is one, and we might add more levels.
        # @n_tries: numer of try/excepts to allow, when try/except is implemented in loops.
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
    if n_tries is None:
        n_tries=10
    n_tries = int(n_tries)
    #
    if hasattr(layer_index, '__len__') and len(layer_index)==1:
        # Just treat this as a scalar
        layer_index = int(layer_index[0])
    #
    if hasattr(layer_index, '__len__'):
        # if it's still list-like, do multiprocessing:
        # ... except it looks like we cannot MPP this at all, at least not with the Nio object; it appears that it does not
        #  support multi-threaded access at all. Can we use the netcdf libraries? Does Nio have an open_parallel() (or something)
        #  function?
        if n_cpu is None:
            n_cpu = mpp.cpu_count()
        layer_index = [int(x) for x in layer_index]
        #
        # we only need one core per layer:
        n_cpu = min(n_cpu, len(layer_index))
        #
        if verbose:
            print('*** instantiating Pool() with {} workers'.format(n_cpu))
            print('*** layer_index: {}'.format(layer_index))
        #
        P = mpp.Pool(n_cpu)
        #
        # we don't need to return anything, so we could probably use a map(), map_async(), or imap_{unordered?"). In the end, I think
        #  these are shorthands, something like map_async().get() is a wrapper around [apply_async()...]; [r.get() ]
        # Anyway, apply_async always works, it's easy to pass parameters to, etc.
        #
        #workers = [P.apply_async(copy_U_from_QBO_layer, kwds={'layer_index':k, 'fpathname_src':fpathname_src, 'fpath_dest':fpath_dest,
        #                 'batch_size':batch_size, 'kt_0':kt_0, 'kt_max':kt_max, 'verbose':verbose, 'n_cpu':1}) for k in layer_index]
        #P.close()
        #P.join()
        #
        # but turns out that in newer versions, we can use starmap(), which is basically magic.
        #  ... except that this process is so IO limited that this gets us nothing unless we make copies of the source file,
        #   so let's serialize:
#        #jobs = P.starmap(copy_U_from_QBO_layer [[k, fpathname_src, fpath_dest, fname_dest, batch_size, kt_0, kt_max, verbose, 1]
#        jobs = P.starmap(copy_U_from_QBO_layer, [[k, fpathname_src, fpath_dest, None, batch_size, kt_0, kt_max, verbose, 1] for k in layer_index])
#        P.close()
#        P.join()
        #
        # for now, hack out of this by just making this a serial loop:
        # TODO: might as well update this to use kwd calling, fpathname_src=fpathname_srce, etc.
        jobs = [copy_U_from_QBO_layer_iosafer(k, fpathname_src, fpath_dest, None, batch_size, kt_0, kt_max, verbose, 1) for k in layer_index]
        #
        return jobs
    #
    #
    if fpathname_src is None:
        fpathname_src = "U_V_T_Z3_plWACCMSC_CTL_122.cam.h2.0001-0202.nc"
    if fpath_dest is None:
        fpath_dest = ''
    #
    #
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
        dim_names = ['time', 'lat', 'lon']
        dim_lens  = [fin.dimensions[s] for s in dim_names]
        n_time, n_pev, n_lat, n_lon = numpy.shape(fin.variables['U'])
        #
    #
        with contextlib.closing(Nio.open_file(fpathname_dest, 'c')) as fout:
            #
            # we should probably abstract the create_dimensions() part, but for now keep it simple:
            #for dn,dl in zip(dim_names, numpy.shape(fin.variables['U'])):
            #    fout.create_dimension(dn, dl)
            fout.create_dimension('time', n_time)
            fout.create_dimension('lat', n_lat)
            fout.create_dimension('lon', n_lon)
            if verbose: print('** DEBUG: [{}:{}]:: dimensions created: {}'.format(os.getppid(), os.getpid(), time.time()-t0))
			
    #
            #
            # set the dimension variable values as well:
            # NOTE: this step may not be necessary; these variables might be brought along by
            #. the principal data, since those data have lat, lon, time listed as their dimensions.
            # TODO: if they are necessary... in the spirit of our 'IO-safer' version, we shold wrap the batching on large variables
            #  inside batching. we might also change this condition to some sort of batchsize/{product of dimensions?}
            #  ALSO: add try-except loops on the batches -- if necessary.
            #  ... but mostly, we should figure out if we need to do this, or if the dimension variables come along with the data var.
            for v_name in dim_names:
                fout.create_variable(v_name, fin.variables[v_name].typecode(), (v_name, ) )
            #
            fout.create_variable('U','d',('time','lat','lon'))
            setattr(fout.variables['U'], 'standard_name', 'pressure')
            setattr(fout.variables['U'], 'units', 'kPa')
            if verbose: print('** DEBUG: [{}:{}]:: variable[U] created (cum time): {}'.format(os.getppid(), os.getpid(), time.time()-t0))
            # Now, close the files so we can do an IO-safer copy.
    #
    # now, assign values to dim and non-dim variables:
    # TODO: we can atually add 'U' (and any other variable, to this loop...
    for v_name, len_var in zip(dim_names, dim_lens):
        #
        #fout.create_variable(v_name, fin.variables[v_name].typecode(), (v_name, ) )
        #
        #len_var = len(fin.variables[v_name])
        #print('*** shape(fin.variables[{}]): '.format(v_name, numpy.shape(fin.variables[v_name])))
        for k in range(0,len_var, batch_size):
            #
            # Add exception handling":
            try_task_completed = False
            k_try = -1
            while k_try < n_tries:
                k_try += 1
                k1 = min(k+batch_size, len_var)
                #
                # try to open files and read/write:
                try:
                    with contextlib.closing(Nio.open_file(fpathname_src, 'r')) as fin:
                        with contextlib.closing(Nio.open_file(fpathname_dest, 'a')) as fout:
                            #
                            if (verbose and len_var>batch_size):
                                print('** DEBUG: copying variables[{}]: {}-{}/{}.'.format(v_name, k,k1, len_var))
                            fout.variables[v_name][k:k1] = (fin.variables[v_name])[k:k1]
                            try_task_completed=True
                            #
                            # if we get this far, we succeeded:
                            k_try = n_tries+1
                            # break the inner (while) loop
                            break
                except Exception as e:
                    # something broke. try a few more times, then throw an exception:
                    # NOTE: we can actually skip this k_try>n_tries handler; just run the warning and then assert after the loop.
                    print('** EXCEPTION: ', e)
                    if k_try >= n_tries:
                        # Enough already! We've tried and tried; throw an exception and quit.
                        break
                        raise Exception('ERROR/EXCEPTION: N>{} IO failures copying variables[{}]'.format(n_tries, v_name))
                    else:
                        # We failed writing the batch, but let's try again:
                        print('** WARNING: [{}/{}] IO failures copying variables[{}]. Trying again.'.format(k_try, n_tries, v_name))
            assert try_task_completed, '** Assertion ERROR/EXCEPTION: N>{} IO failures copying variables[{}]'.format(n_tries, v_name)
            #
        if verbose: print('** DEBUG: [{}:{}]:: var {} written'.format(os.getppid(), os.getpid(), v_name))
    if verbose: print('** DEBUG: (p)pid[{}:{}]:: variables created and assigned (cumulative time): {}'.format(os.getppid(), os.getpid(), time.time()-t0))
    #
#    with contextlib.closing(Nio.open_file(fpathname_dest, 'w')) as fout:
#        #
#        # TODO: do we need to populate the dimension variables before we create a variable with those dimensions. In other words,
#        #  can we move this "create_variable() " block up to the block where we create dimension variables -- and save an open() block?
#        fout.create_variable('U','d',('time','lat','lon'))
#        setattr(fout.variables['U'], 'standard_name', 'pressure')
#        setattr(fout.variables['U'], 'units', 'kPa')
#        if verbose: print('** DEBUG: [{}:{}]:: variable[U] created (cum time): {}'.format(os.getppid(), os.getpid(), time.time()-t0))
#        #
    #
    # now write file:
    t1=time.time()
    t2=time.time()
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
        k1 = min(k+batch_size, n_time)
        #
        # we often get, what appear to be, random failures here, probably due to IO interruption, errors, timeouts, etc.,
        #  so add exception handling.
        # TODO: we're controlling this exceptin handling 3 ways; we should probably just pick one.
        k_try = -1
        # use a boolean flag to determine if the task completed, and so whether or not to throw an exception.
        try_task_completed=False
        while k_try<n_tries:
            k_try+=1
            #
            try:
                # open the fiels and do stuff:
                with contextlib.closing(Nio.open_file(fpathname_src, 'r')) as fin:
                    with contextlib.closing(Nio.open_file(fpathname_dest, 'w')) as fout:
                        #
                        #print('*** k:k1: {}:{}'.format(k,k1))
                        fout.variables['U'][k:k1] = fin.variables['U'][k:k1, layer_index,:,:]
                        if verbose:
                            t2 = time.time()
                            print('** DEBUG: [{}:{}]:: begin  k={}/{} batches [{}:{}]/{} :: {}.'.format(os.getppid(), os.getpid(), j, n_batches, k, k1, n_time, t2-t1))
                            t1=t2
                        #
                        # if we get here without an exception, it worked, so we want to exit the loop. We can either set up our while statment to fail, or use
                        #  break... or both.
                        try_task_completed = True
                        k_try = n_tries+1
                        break
                        #
            except:
                if k_try >= n_tries:
                    # Enough already! We've tried and tried; throw an exception and quit.
                    break
                    raise Exception('ERROR/EXCEPTION: N>{} IO failures copying variables[{}]'.format(n_tries, 'U'))
                else:
                    # We failed writing the batch, but let's try again:
                    print('** WARNING: [{}/{}] IO failures copying variables[U]. Trying again.'.format(k_try, n_tries))
                #
        assert try_task_completed, '** Assertion ERROR/EXCEPTION: N>{} IO failures copying variables[{}]'.format(n_tries, 'U')
        # and if we missed an exception, catch it here:
        if not try_task_completed:
            raise Exception('ERROR/EXCEPTION: N>{} IO failures copying variables[{}] (exception trapped outside loop, by try_task_completed failure)'.format(n_tries, 'U'))
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
    parser.add_argument('--n_tries', dest='n_tries')
    #
    args = parser.parse_args()
    print('accumulate: ', args.accumulate(args.layer_indices))
    print('indices: ', args.layer_indices)
    print('*** ***')
    print('src_pathname: ', args.src_pathname)
    print('kt_0, kt_max: {} : {}'.format(args.kt_0, args.kt_max))
    print('dest_path: ', args.fpath_dest)
    print('dest_fname: ', args.fname_dest)
    print('batch_size: ', args.batch_size)
    print('verbose: ', args.verbose)
    print('n_cpu: ', args.n_cpu)
    print('n_tries: ', args.n_tries)
    #
    layer_indices = args.layer_indices
    if len(layer_indices)==1:
        layer_indices=layer_indices[0]
    #kt_max = args.kt_max
    #print('*** kt_max: ', args.kt_max)
    #
    # we'll call the parsing function like this:
    # def copy_U_from_QBO_layer(layer_index=0, fpathname_src='', fpath_dest='', fname_dest=None, batch_size=None,
    #		kt_0=0, kt_max=None, verbose=0, n_cpu=None):
    # Note: we have added quasi-recuersive parallelization to copy_U_from_QBO_layer(), so all we do here is pass variables...
    #x = copy_U_from_QBO_layer(layer_index=layer_indices, fpathname_src=args.src_pathname, fpath_dest=args.fpath_dest, fname_dest=args.fname_dest, batch_size=args.batch_size,
x = args.f_QBO_copy(layer_index=layer_indices, fpathname_src=args.src_pathname, fpath_dest=args.fpath_dest, fname_dest=args.fname_dest, batch_size=args.batch_size, kt_0=args.kt_0, kt_max=args.kt_max, verbose=args.verbose, n_cpu=int(args.n_cpu), n_tries=args.n_tries)
