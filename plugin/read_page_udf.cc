#include<assert.h>
#include<ctype.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>


#include"mysql.h"
#include "my_sys.h"
#include"mysql/udf_registration_types.h"
#include "mysql/psi/mysql_memory.h"


#include "trx0sys.h"
#include "fil0fil.h"
#include "mtr0mtr.h"
#include "mach0data.h"

/*
PSI_memory_key key_memory_read_page_context;


#ifdef HAVE_PSI_INTERFACE

static PSI_memory_info all_read_page_memory[] = {
    {&key_memory_read_page_context, "read_page_context", 0, 0,
     PSI_DOCUMENT_ME}};

static void init_read_page_psi_keys() {
  const char *category = "read_page";
  int count;

  count = static_cast<int>(array_elements(all_read_page_memory));
  mysql_memory_register(category, all_read_page_memory, count);
}

#endif
*/

namespace read_page_var{
	const static byte validPage=0x0;
	const static byte invalidePage=0x1;
	void * my_alloc(size_t size){
		fprintf(stderr,"Memory allocated :%lu\n",size);
		return malloc(size);
	}
}

extern "C" bool read_page_init(UDF_INIT * initid, UDF_ARGS * args, char * message){

/*
#ifdef HAVE_PSI_INTERFACE
	init_read_page_psi_keys();
#endif
*/

	if ( args->arg_count !=2 ){
		strcpy(message, "This function requires 2 argument (space_id_t, pageno)");
		return true;
	}

	args->arg_type[0] = INT_RESULT;
	args->arg_type[1] = INT_RESULT;


	initid->ptr=(char *)read_page_var::my_alloc(UNIV_PAGE_SIZE+4+4);

	if(!initid->ptr){
		strcpy(message, "Memory allocation failure");
		return true;
	}
	fprintf(stderr, "Memory allocated successfully: %lu",sizeof(initid->ptr));
	memset(initid->ptr, 0, UNIV_PAGE_SIZE);
	initid->max_length=UNIV_PAGE_SIZE;

	return false;

}

extern "C" char * read_page(UDF_INIT * initid, UDF_ARGS * args, char * result,unsigned long * length, char * is_null, char * error){


	space_id_t space_id=(space_id_t)(*(long long *)args->args[0]);
	page_no_t page_no=(page_no_t) (*(long long *) args->args[1]);
	*is_null=0;

	bool found=false; int len=0;

	byte flag=read_page_var::invalidePage;
	char msg[100];
	result=(char *)initid->ptr;
	byte * wr_ptr=(byte *)result;


	page_id_t page_id(space_id,page_no);
	const page_size_t page_size =fil_space_get_page_size(space_id, &found);

	fprintf(stderr, "Read page(%d,%d) requested - found: %d\n",space_id, page_no,found);
	if(!found ){
		sprintf(msg,"Tablespace not found");
	}else{

		page_no_t size=fil_space_get_size(space_id);
		if(size < page_no){
			sprintf(msg,"Page out of range; max-page no: %d, requested: %d",size,page_no );
		}else{
			mtr_t mtr;
			mtr_start(&mtr);
			buf_block_t *block;
			block = buf_page_get(page_id, page_size, RW_X_LATCH, &mtr);
			//buf_block_dbg_add_level(block, SYNC_NO_ORDER_CHECK);

			mach_write_to_1(wr_ptr,read_page_var::validPage);
			wr_ptr++;
			wr_ptr+=mach_write_compressed(wr_ptr,block->page.size.physical());
			memcpy(wr_ptr,block->frame,block->page.size.physical());

			//buf_page_print(wr_ptr,block->page.size,BUF_PAGE_PRINT_NO_CRASH);

			wr_ptr+=block->page.size.physical();
			mtr_commit(&mtr);
			goto end;
		}
	}


	mach_write_to_1(wr_ptr,flag);
	wr_ptr++;

	len=strlen(msg);
	wr_ptr+=mach_write_compressed(wr_ptr,len);

	memcpy(wr_ptr,msg,len);
	wr_ptr+=len;

end:
    *length=((char*) wr_ptr) -result;
	fprintf(stderr,"Length: %lu,  Data:%s\n",*length,result);
	return result;
}



extern "C" void read_page_deinit(UDF_INIT * initid) {
	if(initid->ptr){
		free(initid->ptr);
	}
}

extern "C" bool decode_page_init(UDF_INIT * initid, UDF_ARGS * args, char * message){

	if ( args->arg_count != 3){
		strcpy(message,"This function requires 3 argument (user,space_id, page_no)");
		return true;
	}

	args->arg_type[0]=STRING_RESULT;
	args->arg_type[1]=INT_RESULT;
	args->arg_type[2]=INT_RESULT;

	initid->max_length=UNIV_PAGE_SIZE;

	initid->ptr=(char*)malloc(UNIV_PAGE_SIZE+4+4);
	if(!initid->ptr){
		strcpy(message,"Memory allocation failed");
		return true;
	}
	return false;
}

extern "C" void decode_page_deinit(UDF_INIT * initid){
	if(initid->ptr){
		free(initid->ptr);
	}
}

extern "C" char * decode_page(UDF_INIT * initid, UDF_ARGS * args, char * result, unsigned long * length , char * is_null, char * error){


	result=(char *)initid->ptr;
	char * user=args->args[0];
	space_id_t space_id=(space_id_t)(*(long long *)args->args[1]);
	page_no_t page_no=(page_no_t)(*(long long *)args->args[2]);

	char query[50], msg[100];
	sprintf(query, "select read_page(%d,%d)",space_id,page_no);

	MYSQL mysql;
	MYSQL_RES * res=NULL;
	MYSQL_ROW row;
	const byte * rptr;
	byte flag=-1;
	int len=0,read_page_no, read_space_id;


	mysql_init(&mysql);
	mysql_options(&mysql,MYSQL_INIT_COMMAND,"set autocommit=0");


	if(!mysql_real_connect(&mysql,"localhost",user,"","jbossdb",3306,NULL,0)){
		sprintf(result,"Host connect failure using user:%s - %s", user,mysql_error(&mysql));
		goto end;
	}
	if(mysql_real_query(&mysql,query,strlen(query))){
		sprintf(result,"Query failure - %s", mysql_error(&mysql));
		goto end;
	}
	res=mysql_use_result(&mysql);
	while((row=mysql_fetch_row(res))!=NULL){
		rptr=(const byte *)row[0];
		flag=*rptr;
		rptr++;
		len=mach_parse_compressed(&rptr,rptr+8);

		if(!flag){
	        read_page_no = mach_read_from_4(rptr + FIL_PAGE_OFFSET);
	        read_space_id = mach_read_from_4(rptr + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
	        sprintf(result, "Flag: %d, Page(%d,%d), Len: %d",flag,read_space_id,read_page_no,len);
		}else{
			memcpy(msg,rptr,len);
			msg[len]='\0';
			sprintf(result,"Failure flag:%d, Msg: (len:%d, txt: %s) ",flag,len,msg);
		}

		rptr+=len;


	}

end:
	if(res!=NULL){
		mysql_free_result(res);
	}
	mysql_close(&mysql);

	*length=strlen(result);
	return result;

}
