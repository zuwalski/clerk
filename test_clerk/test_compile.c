/* 
    Clerk application and storage engine.
    Copyright (C) 2008  Lars Szuwalski

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

/*
	TEST Compiler
*/
#include "test.h"
#include "../cle_core/cle_compile.h"
#include <stdio.h>
#include <time.h>

///////////////////////////////////////////////////////
// TEST Scripts

static char test1[] = 
":a)"
"	if :a > 1 do"
"		1 + 2 * 3 / :a"
"	elseif :a = 0 do"
"		:a"
"	end";

static char test2[] = 
":a)"
"	var :b = 0;"
"	while :a > 0 do"
"		var :c = "
"			do"
"				:a and :b or :c"
"				break"
"			until :a = 0 end;"
"	"
"		do"
"			var :d = 0;"
"			break"
"		end"
"	end";

static char test3[] = 
":a,:b)"
"	:a{a,b= 1+1;c{['a' 'b' :b]}} "
"handle a,:c do"
"	:c "
"handle v do"
"	'end'";

static char test4[] = 
":a,:b)"
"	pipe fun3(:a,:b) do "
"		fun3(:a)"
"	end"
"	"
"	:b.each a{+*.:1,-:2.:3} do"
"		'each'"
"	end";

static char test5[] = 
":a,:b)"
"   fun1(1)"
"	if fun2(,) do "
"		fun3()"
"	end "
"   b.d = c;"
"   b.d = c d;"
"   var :1 = fun4() d;"
"   var :2 = a b c;"
"   #var :3,:4 = a,b;\n"
"   :a"
"	";

static char test6[] = 
":a)"
"	if :a do"
"		:a :a"
"	elseif if :a do 1 else 0 end do"
"	#elseif 0 do\n"
"		:a;"
"		:a :a;"
"		:a"
"	end";

static char test7[] = 
":a)"
"   while 1 do"
"      :a :a;"
"      :a :a"
"   end"
"      "
"   var :b ="
"      while 0 do"
"       :a"
"      end;"
"      ";

static char test8[] = 
")"
" nr = nr + 1;"
" 'hallo world nr ' nr  \n"
;

static char test9[] = 
":x,:y,:z)"
" if :y < :x do "
"  tak( tak(:x - 1,:y,:z), tak(:y - 1,:z,:x), tak(:z - 1,:x,:y) )"
" else :z end";
//":1)"
//" if :1 > 1 do start(:1 - 1) + start(:1 - 2) else :1 end";

static char fib[] = 
":fib,:n)"
" if :n > 1 do :fib(:fib,:n - 1) + :fib(:fib,:n - 2) else 1 end";

static char neg[] = 
":in)"
"-( a  - -c )";

static char out[] = 
":in)"
":in = a b c;"
"a b :in "
" b if a b do c d end e";

static char trouble[] =
") d call() me() (call it)";

static char assign[] = 
":a,:b) "
" var :c = 1,:d = 2;"
" :a,:b = :b,:a, :b = :a;"
" if :c do :a :b else :d end"
" f = :a :b;"
;

static char mixed[] = 
")"
" if 0 do {a,b,c} v end a b c "
;

static char loops[] = 
") var :ipt = read(); 'Resp start> ' while :ipt do :ipt ' <next >' then :ipt = read(); end ' <resp end'";

static char props[] = 
":newval) 'prop1 was: ' prop1 ' set to ' :newval then prop1 = :newval;  ' read prop1 ' prop1";

static void _do_test(task* t, char* test, int length)
{
	st_ptr dest,src,tmp;

	puts("test\n");

	st_empty(t,&src);
	tmp = src;

	st_insert(t,&tmp,test,length - 1);

	//puts(test);

	st_empty(t,&dest);
	tmp = dest;
//	if(cmp_method(t,&dest,&src,&_test_pipe_stdout,0,1) == 0)
//		_rt_dump_function(t,&tmp);
}

#define CMPTEST(f) _do_test(t,(f),sizeof((f)))

void test_compile_c()
{
	task* t = tk_create_task(0,0);

	//_do_test(t,test1,sizeof(test1));
	//_do_test(t,test2,sizeof(test2));
	//_do_test(t,test3,sizeof(test3));
	//_do_test(t,test4,sizeof(test4));
	//_do_test(t,test5,sizeof(test5));
	//_do_test(t,test6,sizeof(test6));
	//_do_test(t,test7,sizeof(test7));
	//_do_test(t,test8,sizeof(test8));
	//_do_test(t,test9,sizeof(test9));
	//CMPTEST(test1);
	//CMPTEST(fib);
	//CMPTEST(neg);
	//CMPTEST(out);
	//CMPTEST(trouble);
	//CMPTEST(assign);
	//CMPTEST(mixed);
	CMPTEST(loops);
	CMPTEST(props);

	tk_drop_task(t);
}
