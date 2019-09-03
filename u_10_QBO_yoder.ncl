; yoder 22 Aug 2019:
; Modify this script to better handle large data files.
; See also a Python and/or Jupyter Notebooks + Python
; version of this script. Note that NOAA et al. are adopting
; Python as their official scripting language; at this time,
; NCL functionality is being integrated into NCL packages and
; extensions.
; For this script, in the short term, there are three basic strategies:
;  1) Just reduce the data footprint. Here, we load the full data set
;   and then parse out a relatively small part of it. How 'bout we just
;   don't load the whole thing?
;  2) Write a loop to batch the job -- read the source data and write to
;   to the output file in smaller batches
;  3) Use a different (disk-memory) hybrid data container. I'd originally
;   thought HDF5, but I'm actually thinking we can just go from one file
;   handler to another.

load "$NCARG_ROOT/lib/ncarg/nclex/gsun/gsn_code.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/shea_util.ncl"

begin

print("doing this")
; Extracts U at 10 hPa and saves, along with lat and lon

; Load files
fn = "U_V_T_Z3_plWACCMSC_CTL_122.cam.h2.0001-0202.nc"
in = addfile(fn,"r")
print("Loaded files")
;
; yoder: I'm pretty sure we can wait on this:
;u = in->U
;print("Got U")
;
; Do we even use these? They're small, but if we're trying to reduce
;  our memory footprint, every bit can help!
lat = in->lat
time = in->time
lon = in->lon
;p = in->lev_p

print("extracted variables")

; Get 10 hPa
;p10 = p(2) ; Test
p10 = in->lev_p(2); Test
;
; yoder: get only the data we need:
;   TODO: questinos: 1) can we nest functon calls?
;     2) can we self-re-assign, so um=f(um)?
;    At least in the short term, this should probably get us what we need.
;;udum = u(:,2,:,:)
;udum = in->U(:,2,:,:)
;um = rm_single_dims(udum)
;
; can we nest these?
;um = rm_single_dims(in->U(:,2,:,:))
;um = rm_single_dims(in->U(,2,:,:))

print("got 10hPa")

varnames = (/"t"/)

delete([/varnames/])

print("saving your work")
;
;;;;;;;;;;;;;;;;;;;;
; Save file
dum = "0001-0202"

filo = "U_10_QBO_" + dum + ".nc"
; get a file handler; in "create" (??) mode.
; NOTE: this will throw an error if the file exists.
system("/bin/rm -f " + filo)
fout = addfile(filo, "c")
;
print("File created")
;
setfileoption(fout,"DefineMode",True)
; Set attributes of output file
fAtt = True
fAtt@creation_date = systemfunc("date")
fileattdef(fout,fAtt)
filedimdef(fout,"time",-1,True)  
print("Set file attributes")
;
; write data to file:
; note: we can probably skip all of the memory variable stuff
; and just write the file to file here.
fout ->u10 = rm_single_dims(in->U(,2,:,:))
;fout ->u10 = um
print("Saved u: " + fout.dimensions.values())
fout ->pl = p10
print("done")

end
